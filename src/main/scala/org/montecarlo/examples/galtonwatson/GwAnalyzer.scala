package org.montecarlo.examples.galtonwatson

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.montecarlo.Analyzer


/**
 * A collection of useful SQL operations on the experiment output Dataset
 *
 * @param gwOutputDS The output Dataset of the Galton-Watson experiment
 */
class GwAnalyzer(val gwOutputDS:Dataset[GwOutput]) extends Analyzer {

  override def getOutputDF() : DataFrame = this.gwOutputDS.toDF()

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
    gwOutputDS.filter($"isFinished" ).filter(!_.isSeedDominant).groupBy("lambda").agg(
      avg($"turn").as("extinctionTime"))
      .orderBy("lambda")
  }
}

