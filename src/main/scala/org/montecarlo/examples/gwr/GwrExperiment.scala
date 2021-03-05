package org.montecarlo.examples.gwr

import org.apache.spark.sql.SaveMode
import org.montecarlo.Implicits._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Output, Parameter}

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
                      seedResourceAcquisitionFitness:Parameter[Double]
                        = ("0.9".toBD to "4.0".toBD by "0.05".toBD).map(_.toDouble),
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
                     trialUniqueId:String) extends Output
object GwrOutput extends Output {
  /**
   * We extract the output parameters from t
   * @param t The trial
   * @return one raw in the output table
   */
  def apply(t:GwrTrial):GwrOutput = new GwrOutput(
    turn = t.turn(),
    seedSurvivalChance = if (t.isSeedDominant) 1.0 else 0.0,
    seedResourceAcquisitionFitness = t.seedNode.gene.resourceAcquisitionFitness,
    isFinished = t.isFinished,
    nrOfSeedNodes =  t.seedNodes().size,
    trialUniqueId = t.trialUniqueId
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
      val confidence = 0.99
      import experiment.spark.implicits._
      val inputDimNames = experiment.input.fetchDimensions().mkString(", ")

      experiment.outputRDD.toDS().createTempView(GwrOutput.name)

      val sqlSeedSurvivalChance: String = s"select $inputDimNames, count(seedSurvivalChance) as trials, " +
        "avg(seedSurvivalChance) as seedSurvivalChance, " +
        s"error(seedSurvivalChance, $confidence) as error " +
        s"from ${GwrOutput.name}  where isFinished==true group by $inputDimNames order by $inputDimNames"
      println("seedSurvivalChanceDF SQL query : " + sqlSeedSurvivalChance)
      val seedSurvivalChanceDF = experiment.spark.sql(sqlSeedSurvivalChance)
      seedSurvivalChanceDF.show(100)

      seedSurvivalChanceDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save("output/GWR_seedSurvivalChance.csv")

      val prolongTrialsTill = 50 //turns

      experiment.spark.table(GwrOutput.name).retroActivelyProlongTrials(prolongTrialsTill)
        .createTempView(GwrOutput.name +"Prolonged")
      val sqlSeedPopulationByTurn: String = s"select seedResourceAcquisitionFitness, turn, " +
        "avg(nrOfSeedNodes) as seedPopulation, " +
        s"error(nrOfSeedNodes, $confidence) as error " +
        s"from ${GwrOutput.name}Prolonged where turn <= $prolongTrialsTill" +
        s" group by seedResourceAcquisitionFitness, turn  order by seedResourceAcquisitionFitness, turn"
      println("seedPopulationByTurnDF SQL query : " + sqlSeedPopulationByTurn)
      val seedPopulationByTurnDF = experiment.spark.sql(sqlSeedPopulationByTurn)
      seedPopulationByTurnDF.show(400)
      seedPopulationByTurnDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save("output/GWR_seedSurvivalChanceByTurn.csv")
      println(s"Distinct trials captured : ${experiment.spark.table(GwrOutput.name).countTrials()}")
      println(s"Distinct turns captured : ${experiment.spark.table(GwrOutput.name).countTurns()}")

      experiment.spark.stop()


    }
  }
}