package org.alakka.galtonwatson

import org.alakka.utils.Time.time
import org.apache.spark.sql.{Dataset, SparkSession}


/**
 * Implementation of the Galton-Watson experiment by various lambda values and Monte-Carlo trials
 *
 * @param name The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param monteCarloMultiplicity The number of trials executed for each lambda in the lambdaRange Seq
 * @param lambdaRange The range of lambda values for the trials that are executed. Each value will executed by monteCarloMultiplicity time  s
 * @param enableInTrialOutputData collection of TrialOutput data  happens after every tick, not only at the end of Trial
 *                                Setting it false gives you some performance boost
 */
class Experiment(val name:String,
                 val monteCarloMultiplicity:Int = 1000,
                 val lambdaRange:IndexedSeq[Double]= Vector(1.0),
                 //val maxPopulationRange:IndexedSeq[Long] = Vector(1000L),
                 val enableInTrialOutputData:Boolean = true

                ) {
/*
  def this(val name:String,
  val monteCarloMultiplicity:Int = 1000
  ) = {

  }*/

  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()

  def generateAllTrialInputs(): Dataset[TrialInput] = {

    import spark.implicits._
    val lambdasDS = this.lambdaRange.map(t => Lambda(t)).toDS()
    val multiplicityDS = (for(i <- 1 to this.monteCarloMultiplicity) yield MultiplicityId(i) ).toDS()
    lambdasDS.crossJoin(multiplicityDS).as[TrialInput]

  }
  def run(): Dataset[TrialOutput] = {

    val trialInputDS = this.generateAllTrialInputs().cache()

    println(s"${this.spark.sparkContext.appName} started with ${trialInputDS.count()} trials")
    println("Spark version : " + this.spark.sparkContext.version)

    import spark.implicits._
    val trialOutputDS:Dataset[TrialOutput] = if (this.enableInTrialOutputData) {
      trialInputDS.flatMap(trialInput => {
          val trial = new Trial(1000, seedNode = new Node(trialInput.lambda))
          var outputList = Seq[TrialOutput]() :+ TrialOutput(trial)
          while (!trial.isFinished) {
            trial.tick()
            outputList = outputList :+ TrialOutput(trial)
          }
          outputList

        })
    } else {
      trialInputDS.map(trialInput =>
        TrialOutput( new Trial(1000, seedNode = new Node(trialInput.lambda)).run()) )
    }
    trialOutputDS
  }







}
object Experiment {

  def main(args : Array[String]): Unit = {

    time {
      val monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 1000
      val lambdaRange = Vector(1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6)

      val experiment = new Experiment("Galton-Watson Experiment", monteCarloMultiplicity, lambdaRange)

      val trialOutputDS = experiment.run().cache()

      val analyzer = new TrialOutputAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(10).show(100)


      analyzer.expectedExtinctionTimesByLambda().show()
      val ticks = analyzer.ticks()
      val trials = analyzer.trials()
      println(s"\n$ticks ticks (smallest unit of time) processed in $trials trials, averaging "
        + f"${ticks.toDouble / trials}%1.1f ticks/trial\n")

      experiment.spark.stop()


    }
  }
}
