package org.montecarlo.examples.gw

import org.apache.spark.sql.SaveMode
import org.montecarlo.Implicits._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Parameter}

/**
 * Input to our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment.
 * Experiment runs all the combinations of these seedResourceAcquisitionFitness and totalResource variations. All fields should have type of
 * Parameter[T]. These parameters can be initialized with a Seq of T object or with the help implicit conversions with
 * T instances directly. These will be converted as a Seq with one element
 *
 * @param lambda        The seedResourceAcquisitionFitness of the Poisson distribution for the random generation of children replicators. It is also the
 *                      expected number of children of the seed replicators and its descendants
 * @param maxPopulation The theoretical work of Galton-Watson is limitless, and the seed node population grows
 *                      exponentially. For simulation though, it is necessary to define cut off point where we
 *                      declare our seed node as a survivor
 */
case class GwInput(
                    lambda: Parameter[Double] = ("0.9".toBD to "3.0".toBD by "0.05".toBD).map(_.toDouble),
                    maxPopulation: Parameter[Long] = Seq(100L)
                  ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[GwOutput])
 */

case class GwOutput(lambda: Double,
                    maxPopulation: Long,
                    seedSurvivalChance: Double,
                    turn: Long,
                    isFinished: Boolean,
                    nrOfSeedNodes: Int,
                    trialUniqueId: String) {
}

object GwOutput {
  def apply(t: GwTrial): GwOutput = new GwOutput(
    lambda = t.seedNode.lambdaForPoisson,
    maxPopulation = t.maxPopulation,
    seedSurvivalChance = if (t.isSeedDominant) 1.0 else 0.0,
    turn = t.turn(),
    isFinished = t.isFinished,
    nrOfSeedNodes = t.livingNodes.size,
    trialUniqueId = t.trialUniqueId
  )
  def name:String = getClass.getSimpleName.split('$').head
}

/**
 * Initiates our  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A> Experiment
 *
 */
object GwExperiment {
  def main(args: Array[String]): Unit = {
    time {

      val experiment = new Experiment[GwInput, GwTrial, GwOutput](
        name = "Galton-Watson Experiment",
        input = GwInput(),
        monteCarloMultiplicity = if (args.length > 0) args(0).toInt else 1000,
        trialBuilderFunction = trialInput => new GwTrial(trialInput.maxPopulation,
          seedNode = new GwNode(trialInput.lambda)),
        outputCollectorBuilderFunction = trial => GwOutput(trial),
        outputCollectorNeededFunction = trial => true
      )
      val confidence = 0.99
      import experiment.spark.implicits._
      val inputDimNames = experiment.input.fetchDimensions().mkString(", ")

      experiment.run().toDS().createTempView(GwOutput.name)

      val sqlSeedSurvivalChance: String = s"select $inputDimNames, count(seedSurvivalChance) as trials, " +
        "avg(seedSurvivalChance) as seedSurvivalChance, " +
        s"error(seedSurvivalChance, $confidence) as error " +
        s"from ${GwOutput.name}  where isFinished==true group by $inputDimNames order by $inputDimNames"
      println("seedSurvivalChanceDF SQL query : " + sqlSeedSurvivalChance)
      val seedSurvivalChanceDF = experiment.spark.sql(sqlSeedSurvivalChance)
      seedSurvivalChanceDF.show(100)
      seedSurvivalChanceDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save("output/GaltonWatson_seedSurvivalChance.csv")

      val prolongTrialsTill = 50 //turns

      experiment.spark.table(GwOutput.name).retroActivelyProlongTrials(prolongTrialsTill)
        .createTempView(GwOutput.name +"Prolonged")
      val sqlSeedPopulationByTurn: String = s"select lambda, turn, " +
        "avg(nrOfSeedNodes) as seedPopulation, " +
        s"error(nrOfSeedNodes, $confidence) as error " +
        s"from ${GwOutput.name}Prolonged where turn < $prolongTrialsTill group by lambda, turn  order by lambda, turn"
      println("seedPopulationByTurnDF SQL query : " + sqlSeedPopulationByTurn)
      val seedPopulationByTurnDF = experiment.spark.sql(sqlSeedPopulationByTurn)
      seedPopulationByTurnDF.show(400)
      seedPopulationByTurnDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save("output/GaltonWatson_seedSurvivalChanceByTurn.csv")

      experiment.spark.stop()
    }
  }
}