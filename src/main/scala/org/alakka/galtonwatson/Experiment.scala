package org.alakka.galtonwatson

import org.alakka.utils.Time.time
import org.alakka.utils.{ProbabilityWithConfidence, Statistics}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}




object Experiment  {
  val conf:SparkConf = new SparkConf().setAppName("Galton-Watson Simulation")
  val spark:SparkSession = SparkSession.builder.config(conf).getOrCreate()

  def generateAllTrialInputs(trialsMultiplier:Int =1000): Dataset[TrialInput] = {

    import spark.implicits._
    val lambdasDS = Lambda.generateAllPossibleValues().toDS()
    val trialsNosDS = TrialNo.generateAllPossibleValues(trialsMultiplier).toDS()
    lambdasDS.crossJoin(trialsNosDS).as[TrialInput]

  }
  // -- calculate and show expected extinction time by lambda
  def showExpectedExtinctionTimesByLambda(trialOutputDS:Dataset[TrialOutput]):Unit = {
    import spark.implicits._
    trialOutputDS.where($"isSeedDominant" === false)
      .groupBy("lambda").agg(
      format_number(avg($"time"),1).as("extinctionTime"))
      .orderBy("lambda").show()
  }

  // -- calculate and show expected extinction time by lambda
  def groupTrialOutputsByLambda(trialOutputDS:Dataset[TrialOutput], confidence:Double = 0.95):Dataset[TrialOutputByLambda] = {

    import spark.implicits._
    val survivalProb = trialOutputDS
      .groupBy("lambda").agg(
      avg($"isSeedDominant".cast("Integer")).as("survivalProbability"),
      stddev($"isSeedDominant".cast("Integer")).as("stdDevProbability"),
      count($"isSeedDominant").as("noOfTrials"),
      sum($"time").as("sumOfTime"))

    val survivalProbConf:Dataset[TrialOutputByLambda] = survivalProb.map {
      row =>
        val lambda = row.getAs[Double](0)
        val survivalProbability =row.getAs[Double](1)
        val stdDevProbability =row.getAs[Double](2)
        val noOfTrials =row.getAs[Long](3)

        val (confidenceIntervalLow, confidenceIntervalHigh)
        = Statistics.confidenceInterval (noOfTrials, survivalProbability, stdDevProbability, confidence)

        TrialOutputByLambda(lambda, ProbabilityWithConfidence(survivalProbability, confidence,confidenceIntervalLow, confidenceIntervalHigh),
          noOfTrials, row.getAs[Long](4) )
    }.sort($"lambda")

    survivalProbConf
  }

  // Something like this to stdout :
  // 8888 ticks (unit of time) processed in 700 trials, averaging 12.7 ticks/trial

  def showPerformanceMetrics(trialOutputDS:Dataset[TrialOutput]):Unit = {
    val sumOfTime = trialOutputDS.agg(sum("time")).first().getAs[Long](0)
    val totalTrials = trialOutputDS.agg(count("time")).first().getAs[Long](0)

    println(s"\n$sumOfTime ticks (unit of time) processed in $totalTrials trials, averaging "+ f"${sumOfTime.doubleValue()/totalTrials}%1.1f ticks/trial\n")
  }

  def main(args : Array[String]): Unit = {

    time {

      val trialsMultiplier:Int = if (args.length > 0)  { args(0).toInt } else 1000

      val trialInputDS = this.generateAllTrialInputs(trialsMultiplier)

      println(s"${spark.sparkContext.appName} started with ${trialInputDS.count()} trials")
      println("Spark version : " + spark.sparkContext.version)
      import spark.implicits._
      val trialOutputDS = trialInputDS
        .map(trialInput =>
          TrialOutput(trialInput.constructTrial().run()) )
        .cache()


      this.showExpectedExtinctionTimesByLambda(trialOutputDS)

      val confidence = 0.99
      println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
      this.groupTrialOutputsByLambda(trialOutputDS, confidence)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))

      this.showPerformanceMetrics(trialOutputDS)
      spark.stop()


    }
  }
}
