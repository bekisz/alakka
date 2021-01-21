package org.montecarlo.examples.gwr

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.montecarlo.Analyzer

/**
 * A collection of useful SQL operations on the experiment output Dataset
 *
 * @param gwrOutputDS The output Dataset of the Galton-Watson experiment
 */
class GwrAnalyzer(val gwrOutputDS:Dataset[GwrOutput]) extends Analyzer {

  override def getOutputDF() : DataFrame = this.gwrOutputDS.toDF()

  /**
   * Calculate the survival probabilities of seedNodes within the given confidence interval grouped by
   * resourceAcquisitionFitness (average number of descendant, the resourceAcquisitionFitness in a Poisson distribution)
   */

  /**
   * Calculates the average population by Lambda and Turn. It only makes sense if each turn is captured
   * @param maxTurn the table will collect data up until this turn number
   * @return DataFrame with the number of seed nodes, grouped by resourceAcquisitionFitness and turn
   */
  def averagePopulationByLambdaAndTime(maxTurn:Int =100 ): Dataset[Row] = {

    import spark.implicits._

    this.gwrOutputDS.filter( _.turn <= maxTurn ).flatMap({ trialOutput =>
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
    gwrOutputDS.filter(_.isFinished).filter(!_.isSeedDominant).groupBy("resourceAcquisitionFitness").agg(
      avg($"turn").as("extinctionTime"))
      .orderBy("resourceAcquisitionFitness")
  }

}

