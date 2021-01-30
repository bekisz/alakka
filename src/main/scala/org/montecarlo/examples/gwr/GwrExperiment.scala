package org.montecarlo.examples.gwr

import org.montecarlo.Implicits._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Parameter}

/**
 * Input to our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment.
 * Experiment runs all the combinations of these seedResourceAcquisitionFitness and totalResource variations.
 * All fields should have type of Parameter[T]. These parameters can be initialized with a Seq of T object or
 * with the help implicit conversions with T instances directly.
 *
 * These will be converted as a Seq with one element
 *
 * @param seedResourceAcquisitionFitness The resource acquisition fitness of seedReplicator
 *                                       the describes the relative resource acquisition capability
 *                                       of the seed node. If it's 20% higher than an other node it means
 *                                       that it has 20% more chance to acquire resource and then to replicate
 * @param totalResource Number of resource units available for all replicators. One resource unit maintains one node.
 */
case class GwrInput(
                      seedResourceAcquisitionFitness:Parameter[Double] = Vector(1.0, 1.1, 1.2, 1.5, 2.0),
                      totalResource:Parameter[Long] = 100L
                  ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[GwrOutput])
 */

case class GwrOutput(turn: Long,
                     seedSurvivalChance: Double,
                     seedResourceAcquisitionFitness: Double,
                     isFinished: Boolean,
                     nrOfSeedNodes:Int,
                     trialUniqueId:String)
object GwrOutput {
  /**
   * We extract the output parameters from t
   * @param t The trial
   * @return one raw in the output table
   */
  def apply(t:GwrTrial):GwrOutput = new GwrOutput(
      t.turn(),
      if (t.isSeedDominant) 1.0 else 0.0,
      t.seedNode.gene.resourceAcquisitionFitness,
      t.isFinished,
      t.seedNodes().size,
      t.trialUniqueId
    )
}

/**
 *  Initiates our  resource limited
 *  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment
 */
object GwrExperiment {
  def main(args : Array[String]): Unit = {
    time {

      val experiment = new Experiment[GwrInput,GwrTrial,GwrOutput](
        name = "Galton-Watson with Resources Experiment",
        input = GwrInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 200,

        trialBuilderFunction = trialInput => new GwrTrial(
          maxResource = trialInput.totalResource,
          seedNode = new GwrNode(Gene(trialInput.seedResourceAcquisitionFitness, label = "seed")),
          nrOfSeedNodes = 1,
          opponentNode = new GwrNode(Gene(1.0, label = "opponent"))),
        outputCollectorNeededFunction = trial => trial.turn %1 ==0 || trial.isFinished,
        outputCollectorBuilderFunction = trial => GwrOutput(trial)
      )
      import experiment.spark.implicits._
      val trialOutputDS = experiment.run().toDS().cache()
      trialOutputDS.show(20)
      val analyzer = new GwrAnalyzer(trialOutputDS)

      println("Confidence Intervals for the survival probabilities")

      trialOutputDS.toDF().groupBy(experiment.input).calculateConfidenceIntervals(
        "seedSurvivalChance",List(0.95,0.99,0.999)).orderBy(experiment.input).show()


      //analyzer.expectedExtinctionTimesByLambda().show()
      val turns = analyzer.turns()
      val trials = analyzer.trials()
      println(s"\n$turns turns processed in $trials trials, averaging "
        + f"${turns.toDouble / trials}%1.1f turns per trial\n")

      experiment.spark.stop()


    }
  }
}