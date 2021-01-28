package org.montecarlo.examples.gw
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Analyzer, Experiment, Input, Parameter}

/**
 * Input to our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment.
 * Experiment runs all the combinations of these resourceAcquisitionFitness and totalResource variations. All fields should have type of
 * Paramater[T]. These parameters can be initialized with a Seq of T object or with the help implicit conversions with
 * T instances directly. These will be converted as a Seq with one element
 *
 * @param lambda The resourceAcquisitionFitness of the Poisson distribution for the random generation of children replicators. It is also the
 *               expected number of children of the seed replicators and its descendants
 * @param maxPopulation The theoretical work of Galton-Watson is limitless, and the seed node population grows
 *                      exponentially. For simulation though, it is necessary to define cut off point where we
 *                      declare our seed node as a survivor
 */
case class GwInput(
                    lambda:Parameter[Double] = Vector(1.2, 1.5, 2.0),
                    maxPopulation:Parameter[Long] = 1000L
                  ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[GwOutput])
 */

case class GwOutput(turn: Long,
                    isSeedDominant: Boolean,
                    lambda: Double,
                    isFinished: Boolean,
                    nrOfSeedNodes:Int,
                    trialUniqueId:String)
object GwOutput {
  def apply(t:GwTrial):GwOutput = new GwOutput(
      t.turn(),
      t.isSeedDominant,
      t.seedNode.lambdaForPoisson,
      t.isFinished,
      t.livingNodes.size,
      t.trialUniqueId
    )
}

/**
 *  Initiates our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment
 *
 */
object GwExperiment {
  def main(args : Array[String]): Unit = {
    time {

      val experiment = new Experiment[GwInput,GwTrial,GwOutput](
        name = "Galton-Watson Experiment",
        input = GwInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 1000,
        trialBuilderFunction = trialInput => new GwTrial( trialInput.maxPopulation,
          seedNode = new GwNode(trialInput.lambda )),
        outputCollectorBuilderFunction = trial => GwOutput(trial),
        outputCollectorNeededFunction = trial =>  trial.isFinished
      )


      import experiment.spark.implicits._
      val trialOutputDS = experiment.run().toDS().cache()
      trialOutputDS.show(20)

      val analyzer = new GwAnalyzer(trialOutputDS)

      analyzer.averagePopulationByLambdaAndTime(10).show(100)

      println("Confidence Intervals for the survival probabilities")
      Analyzer.calculateConfidenceIntervalsFromGroups(trialOutputDS
        .toDF().withColumn("survivalChance", $"seedSurvivalChance".cast("Integer"))
        .groupBy("lambda"),
        "survivalChance",List(0.95,0.99,0.999)).orderBy("lambda").show()

      val turns = analyzer.turns()
      val trials = analyzer.trials()
      println(s"\n$turns turns processed in $trials trials, averaging "
        + f"${turns.toDouble / trials}%1.1f turns per trial\n")

      experiment.spark.stop()


    }
  }
}