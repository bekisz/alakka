package org.montecarlo.examples.galtonwatson

import org.montecarlo.utils.{ProbabilityWithConfidence, Statistics}
import org.apache.spark.sql.functions.{avg, count, stddev, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class TrialOutputByLambda(lambda:Double,
                               probabilityWithConfidence:ProbabilityWithConfidence,
                               noOfTrials:Long, sumOfTime:Long) {
  override def toString: String =
    s"  - P(survival|lambda=$lambda) = ${probabilityWithConfidence.toString}"
}

/**
 * A collection of useful SQL operations on the experiment output Dataset
 *
 * @param gwOutputDS The output Dataset of the Galton-Watson experiment
 */
class GwAnalyzer(val gwOutputDS:Dataset[GwOutput]) {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  /**
   * Calculate the survival probabilities of seedNodes within the given confidence interval grouped by
   * lambda (average number of descendant, the lambda in a Poisson distribution)
   */
  def survivalProbabilityByLambda(confidence: Double = 0.98): Dataset[TrialOutputByLambda] = {
    println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
    import spark.implicits._
    val survivalProb = gwOutputDS.filter(_.isFinished)
      .groupBy("lambda").agg(
      avg($"isSeedDominant".cast("Integer")).as("survivalProbability"),
      stddev($"isSeedDominant".cast("Integer")).as("stdDevProbability"),
      count($"isSeedDominant").as("noOfTrials"),
      sum($"turn").as("sumOfTime"))

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
   * Calculates the average population by Lambda and Turn. It only makes sense if each turn is captured
   * @param maxTurn the table will collect data up until this turn number
   * @return DataFrame with the number of seed nodes, grouped by lambda and turn
   */
  def averagePopulationByLambdaAndTime(maxTurn:Int =100 ): Dataset[Row] = {

    import spark.implicits._

    this.gwOutputDS.filter( _.turn <= maxTurn ).flatMap({ trialOutput =>
      var listOutput = trialOutput :: List[GwOutput]()
      if (trialOutput.isFinished) {
        listOutput = listOutput :::
          (trialOutput.turn +1 to maxTurn).map(newTime => trialOutput.copy(turn = newTime)).toList
      }
      listOutput
    })
      .groupBy("lambda", "turn").agg(
      avg($"nrOfSeedNodes").as("avgOfSeedNodes"))
      .orderBy("lambda","turn")
  }

  /**
   * Calculates the mean extinction turn when a seed got extinct
   *
   * @return Row with lambda, extinctionTime
   */
  def expectedExtinctionTimesByLambda(): Dataset[Row]= {
    import spark.implicits._
    gwOutputDS.filter(_.isFinished).filter(!_.isSeedDominant).groupBy("lambda").agg(
      avg($"turn").as("extinctionTime"))
      .orderBy("lambda")
  }

  /**
   * @return # of turns summed in the experiment
   */
  def turns() : Long = {
    gwOutputDS.filter(_.isFinished).select(sum("turn")).first().getAs[Long](0)
  }

  /**
   * @return # of trial executed
   */
  def trials() : Long = {
    gwOutputDS.filter(_.isFinished).count()
  }

}

