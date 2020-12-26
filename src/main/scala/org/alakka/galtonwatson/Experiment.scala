package org.alakka.galtonwatson

import org.alakka.utils.Time.time
import org.alakka.utils.{ProbabilityWithConfidence, Statistics}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}



class Experiment(val name:String, val multiplicity:Int = 1000, val lambdaRange:IndexedSeq[Double]= Vector(1.0)) {

  val conf: SparkConf = new SparkConf().setAppName(name)
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()


  def generateAllTrialInputs(): Dataset[TrialInput] = {

    import spark.implicits._
    val lambdasDS = this.lambdaRange.map(t => Lambda(t)).toDS()
    val multiplicityDS = (for(i <- 1 to this.multiplicity) yield MultiplicityId(i) ).toDS()
    lambdasDS.crossJoin(multiplicityDS).as[TrialInput]

  }
  def run(): Dataset[TrialOutput] = {

    val trialInputDS = this.generateAllTrialInputs().cache()

    println(s"${this.spark.sparkContext.appName} started with ${trialInputDS.count()} trials")
    println("Spark version : " + this.spark.sparkContext.version)
    import spark.implicits._
    trialInputDS
      //.map(trialInput =>
      //  TrialOutput( new Trial(maxPopulation = 1000, seedNode = new Node(trialInput.lambda)).run()) )
       .flatMap(trialInput => {
          val trial = new Trial(maxPopulation = 1000, seedNode = new Node(trialInput.lambda))
          var outputList = Seq[TrialOutput]() :+ TrialOutput(trial)
          while ( !trial.isFinished)  {
             trial.tick()
            outputList = outputList :+ TrialOutput(trial)
          }
         outputList

        }  )

  }

  def showAveragePopulationByTime(trialOutputDS: Dataset[TrialOutput], maxTime:Int =20 ): Unit = {

    import spark.implicits._

    trialOutputDS.where($"time" <= maxTime ).flatMap({ trialOutput =>
      var listOutput = trialOutput :: List[TrialOutput]()
      if (trialOutput.isFinished) {
        listOutput = listOutput :::
          (trialOutput.time +1 to maxTime).map( newTime => trialOutput.copy(time = newTime)).toList
      }
      listOutput
    })
      .groupBy("time").agg(
      format_number(avg($"nrOfSeedNodes"), 1).as("population"))
      .orderBy("time").show(maxTime)
  }

  // -- calculate and show expected extinction time by lambda
  def showExpectedExtinctionTimesByLambda(trialOutputDS: Dataset[TrialOutput]): Unit = {
    import spark.implicits._
    trialOutputDS.where($"isSeedDominant" === false)
      .groupBy("lambda").agg(
      format_number(avg($"time"), 1).as("extinctionTime"))
      .orderBy("lambda").show()
  }

  // -- calculate and show expected extinction time by lambda
  def groupTrialOutputsByLambda(trialOutputDS: Dataset[TrialOutput], confidence: Double = 0.98): Dataset[TrialOutputByLambda] = {
    println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
    import spark.implicits._
    val survivalProb = trialOutputDS
      .groupBy("lambda").agg(
      avg($"isSeedDominant".cast("Integer")).as("survivalProbability"),
      stddev($"isSeedDominant".cast("Integer")).as("stdDevProbability"),
      count($"isSeedDominant").as("noOfTrials"),
      sum($"time").as("sumOfTime"))

    val survivalProbConf: Dataset[TrialOutputByLambda] = survivalProb.map {
      row =>
        val lambda = row.getAs[Double](0)
        val survivalProbability = row.getAs[Double](1)
        val stdDevProbability = row.getAs[Double](2)
        val noOfTrials = row.getAs[Long](3)

        val (confidenceIntervalLow, confidenceIntervalHigh)
        = Statistics.confidenceInterval(noOfTrials, survivalProbability, stdDevProbability, confidence)

        TrialOutputByLambda(lambda, ProbabilityWithConfidence(survivalProbability, confidence, confidenceIntervalLow, confidenceIntervalHigh),
          noOfTrials, row.getAs[Long](4))
    }.sort($"lambda")

    survivalProbConf
  }

  // Something like this to stdout :
  // 8888 ticks (unit of time) processed in 700 trials, averaging 12.7 ticks/trial

  def showPerformanceMetrics(trialOutputDS: Dataset[TrialOutput]): Unit = {
    val sumOfTime = trialOutputDS.agg(sum("time")).first().getAs[Long](0)
    val totalTrials = trialOutputDS.agg(count("time")).first().getAs[Long](0)

    println(s"\n$sumOfTime ticks (unit of time) processed in $totalTrials trials, averaging " + f"${sumOfTime.doubleValue() / totalTrials}%1.1f ticks/trial\n")
  }
}
object Experiment {

  def main(args : Array[String]): Unit = {

    time {
      val multiplicity = if (args.length > 0)  { args(0).toInt } else 1000
      val lambdaRange = Vector(1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6)
      //val lambdaRange2 = 1.0 to 1.6 by 0.1
      val experiment = new Experiment("Galton-Watson Experiment", multiplicity, lambdaRange)

      val trialOutputDS = experiment.run().cache()
      import experiment.spark.implicits._
      experiment.showAveragePopulationByTime(trialOutputDS.filter(_.lambda==1.6),100)

      val finishedTrialOutputDS = trialOutputDS.filter(trialOutput => trialOutput.isFinished).cache()

      experiment.showExpectedExtinctionTimesByLambda(finishedTrialOutputDS)

      experiment.groupTrialOutputsByLambda(finishedTrialOutputDS, confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      experiment.showPerformanceMetrics(finishedTrialOutputDS)
      experiment.spark.stop()


    }
  }
}
