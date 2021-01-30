package org.montecarlo.examples.replicator

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Dataset, Row}
import org.montecarlo.MvDataFrame

/**
 * A collection of useful SQL operations on the experiment output Dataset
 *
 * @param gwrOutputDS The output Dataset of the Galton-Watson experiment
 */
class ReplicatorAnalyzer(val gwrOutputDS:Dataset[ReplicatorOutput]) extends MvDataFrame(gwrOutputDS.toDF()) {

  /**
   * Calculate the survival probabilities of seedReplicators within the given confidence interval grouped by
   * seedResourceAcquisitionFitness (average number of descendant, the seedResourceAcquisitionFitness in a Poisson distribution)
   */

  /**
   * Calculates the average population by Lambda and Turn. It only makes sense if each turn is captured
   * @param maxTurn the table will collect data up until this turn number
   * @return DataFrame with the number of seed replicators, grouped by seedResourceAcquisitionFitness and turn
   */
  def averagePopulationByLambdaAndTime(maxTurn:Int =100 ): Dataset[Row] = {

    import spark.implicits._

    this.gwrOutputDS.filter( _.turn <= maxTurn ).flatMap({ trialOutput =>
      var listOutput = trialOutput :: List[ReplicatorOutput]()
      if (trialOutput.isFinished) {
        listOutput = listOutput :::
          (trialOutput.turn +1 to maxTurn).map(newTime => trialOutput.copy(turn = newTime)).toList
      }
      listOutput
    })
      .groupBy("seedResourceAcquisitionFitness", "turn").agg(
      avg($"nrOfSeedReplicators").as("avgOfSeedNodes"))
      .orderBy("seedResourceAcquisitionFitness","turn")
  }

  /**
   * Calculates the mean extinction turn when a seed got extinct
   *
   * @return Row with seedResourceAcquisitionFitness, extinctionTime
   */
  def expectedExtinctionTimesByLambda(): Dataset[Row]= {
    import spark.implicits._
    gwrOutputDS.filter(_.isFinished).filter(_.seedSurvivalChance<0.00000001).groupBy("seedResourceAcquisitionFitness").agg(
      avg($"turn").as("extinctionTime"))
      .orderBy("seedResourceAcquisitionFitness")
  }

}

