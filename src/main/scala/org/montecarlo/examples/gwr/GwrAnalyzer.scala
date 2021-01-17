package org.montecarlo.examples.gwr

import org.apache.spark.sql.functions.{avg, count, stddev, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.montecarlo.utils.{ProbabilityWithConfidence, Statistics}

case class TrialOutputByLambda(resourceAcquisitionFitness:Double,
                               probabilityWithConfidence:ProbabilityWithConfidence,
                               noOfTrials:Long, sumOfTime:Long) {
  override def toString: String =
    s"  - P(survival|resourceAcquisitionFitness=$resourceAcquisitionFitness) = ${probabilityWithConfidence.toString}"
}

/**
 * A collection of useful SQL operations on the experiment output Dataset
 *
 * @param gwOutputDS The output Dataset of the Galton-Watson experiment
 */
class GwrAnalyzer(val gwOutputDS:Dataset[GwrOutput]) {
  val spark: SparkSession = SparkSession.builder.getOrCreate()

  /**
   * Calculate the survival probabilities of seedNodes within the given confidence interval grouped by
   * resourceAcquisitionFitness (average number of descendant, the resourceAcquisitionFitness in a Poisson distribution)
   */
  def survivalProbabilityByLambda(confidence: Double = 0.98): Dataset[TrialOutputByLambda] = {
    println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
    import spark.implicits._
    val survivalProb = gwOutputDS.filter(_.isFinished)
      .groupBy("resourceAcquisitionFitness").agg(
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
    }.sort($"resourceAcquisitionFitness")

    survivalProbConf
  }

  /**
   * Calculates the average population by Lambda and Turn. It only makes sense if each turn is captured
   * @param maxTurn the table will collect data up until this turn number
   * @return DataFrame with the number of seed nodes, grouped by resourceAcquisitionFitness and turn
   */
  def averagePopulationByLambdaAndTime(maxTurn:Int =100 ): Dataset[Row] = {

    import spark.implicits._

    this.gwOutputDS.filter( _.turn <= maxTurn ).flatMap({ trialOutput =>
      var listOutput = trialOutput :: List[GwrOutput]()
      if (trialOutput.isFinished) {
        listOutput = listOutput :::
          (trialOutput.turn +1 to maxTurn).map(newTime => trialOutput.copy(turn = newTime)).toList
      }
      listOutput
    })
      .groupBy("resourceAcquisitionFitness", "turn").agg(
      avg($"nrOfSeedNodes").as("avgOfSeedNodes"))
      .orderBy("resourceAcquisitionFitness","turn")
  }

  /**
   * Calculates the mean extinction turn when a seed got extinct
   *
   * @return Row with resourceAcquisitionFitness, extinctionTime
   */
  def expectedExtinctionTimesByLambda(): Dataset[Row]= {
    import spark.implicits._
    gwOutputDS.filter(_.isFinished).filter(!_.isSeedDominant).groupBy("resourceAcquisitionFitness").agg(
      avg($"turn").as("extinctionTime"))
      .orderBy("resourceAcquisitionFitness")
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

