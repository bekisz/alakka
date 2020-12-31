package org.alakka.galtonwatson

import org.alakka.utils.{ProbabilityWithConfidence, Statistics}
import org.apache.spark.sql.functions.{avg, count, stddev, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class TrialOutputByLambda(lambda:Double,
                               probabilityWithConfidence:ProbabilityWithConfidence,
                               noOfTrials:Long, sumOfTime:Long) {
  override def toString: String =
    s"  - P(survival|lambda=$lambda) = ${probabilityWithConfidence.toString}"
}

class TrialOutputAnalyzer( val trialOutputDS:Dataset[TrialOutput]) {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  // -- calculate and show expected extinction time by lambda
  def survivalProbabilityByLambda(confidence: Double = 0.98): Dataset[TrialOutputByLambda] = {
    println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
    import spark.implicits._
    val survivalProb = trialOutputDS.filter(_.isFinished)
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

  /**
   * Calulates the average
   * @param maxTime
   * @return
   */
  def averagePopulationByLambdaAndTime(maxTime:Int =100 ): Dataset[Row] = {

    import spark.implicits._

    this.trialOutputDS.filter( _.time <= maxTime ).flatMap({ trialOutput =>
      var listOutput = trialOutput :: List[TrialOutput]()
      if (trialOutput.isFinished) {
        listOutput = listOutput :::
          (trialOutput.time +1 to maxTime).map( newTime => trialOutput.copy(time = newTime)).toList
      }
      listOutput
    })
      .groupBy("lambda", "time").agg(
      avg($"nrOfSeedNodes").as("avgOfSeedNodes"))
      .orderBy("lambda","time")
  }

  /**
   * Calculates the mean extinction time when a seed got extinct
   *
   * @return Row with lambda, extinctionTime
   */
  def expectedExtinctionTimesByLambda(): Dataset[Row]= {
    import spark.implicits._
    trialOutputDS.filter(_.isFinished).filter(!_.isSeedDominant).groupBy("lambda").agg(
      avg($"time").as("extinctionTime"))
      .orderBy("lambda")
  }

  // Something like this to stdout :
  // 8888 ticks (unit of time) processed in 700 trials, averaging 12.7 ticks/trial

  def showPerformanceMetrics(trialOutputDS: Dataset[TrialOutput]): Unit = {
    val sumOfTime = trialOutputDS.agg(sum("time")).first().getAs[Long](0)
    val totalTrials = trialOutputDS.agg(count("time")).first().getAs[Long](0)

  }
  def ticks() : Long = {
    trialOutputDS.filter(_.isFinished).select(sum("time")).first().getAs[Long](0)
  }
  def trials() : Long = {
    trialOutputDS.filter(_.isFinished).count()
  }

}

