package org.montecarlo.examples.gwr

import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Parameter}

/**
 * Input to our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment.
 * Experiment runs all the combinations of these resourceAcquisitionFitness and totalResource variations. All fields should have type of
 * Parameter[T]. These parameters can be initialized with a Seq of T object or with the help implicit conversions with
 * T instances directly. These will be converted as a Seq with one element
 *
 * @param seedResourceAcquisitionFitness The resourceAcquisitionFitness of the Poisson distribution for the random generation of children nodes. It is also the
 *               expected number of children of the seed nodes and its descendants
 * @param totalResource The theoretical work of Galton-Watson is limitless, and the seed node population grows
 *                      exponentially. For simulation though, it is necessary to define cut off point where we
 *                      declare our seed node as a survivor
 */
case class GwwrInput(
                      seedResourceAcquisitionFitness:Parameter[Double] = Vector(1.0, 1.1, 1.2, 1.5, 2.0),
                      totalResource:Parameter[Long] = 100L
                  ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[GwOutput])
 */

case class GwrOutput(turn: Long,
                     isSeedDominant: Boolean,
                     resourceAcquisitionFitness: Double,
                     isFinished: Boolean,
                     nrOfSeedNodes:Int,
                     trialUniqueId:String)
object GwrOutput {
  def apply(t:GwrTrial):GwrOutput = new GwrOutput(
      t.turn(),
      t.isSeedDominant,
      t.seedNode.gene.resourceAcquisitionFitness,
      t.isFinished,
      t.seedNodes().size,
      t.trialUniqueId
    )
}

/**
 *  Initiates our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment
 *
 */
object GwrExperiment {
  def main(args : Array[String]): Unit = {
    time {

      val experiment = new Experiment[GwwrInput,GwrTrial,GwrOutput](
        name = "Galton-Watson with Resources Experiment",
        input = GwwrInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 20000,
        outputCollectorBuilderFunction = trial => GwrOutput(trial),

        trialBuilderFunction = trialInput => new GwrTrial(
          maxResource = trialInput.totalResource,
          seedNode = new GwrNode(Gene(trialInput.seedResourceAcquisitionFitness, label = "seed")),
          nrOfSeedNodes = 1,
          opponentNode = new GwrNode(Gene(1.0, label = "opponent"))),
        outputCollectorNeededFunction = trial => trial.turn %1 ==0 || trial.isFinished
      )
      import experiment.spark.implicits._
      val trialOutputDS = experiment.run().toDS().cache()
      trialOutputDS.show(20)

      val analyzer = new GwrAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(30).show(100)


      //analyzer.expectedExtinctionTimesByLambda().show()
      val turns = analyzer.turns()
      val trials = analyzer.trials()
      println(s"\n$turns turns processed in $trials trials, averaging "
        + f"${turns.toDouble / trials}%1.1f turns per trial\n")

      experiment.spark.stop()


    }
  }
}