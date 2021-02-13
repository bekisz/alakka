package org.montecarlo.examples.replicator
import org.apache.spark.sql.SaveMode
import org.montecarlo.Implicits._
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Output, Parameter}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Logger
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
case class ReplicatorInput(

                            seedResourceAcquisitionFitness:Parameter[Double] =1.8,
                            seedResilience:Parameter[Double] = Seq(0.0,0.5),
                            seedMutationProbability:Parameter[Double]
                            =("0.0".toBD to "0.5".toBD by "0.1".toBD).map(_.toDouble),
                            totalResource:Parameter[Long] = 100L
                   ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[ReplicatorOutput])
 */

case class ReplicatorOutput(turn: Long,
                            seedSurvivalChance: Double,
                            seedResourceAcquisitionFitness: Double,
                            seedResilience: Double,
                            seedMutationProbability : Double,
                            isFinished: Boolean,
                            nrOfSeedNodes:Int,
                            trialUniqueId:String) extends Output {
}
object ReplicatorOutput extends Output  {

  // assertInputConformity(ClassTag(ReplicatorInput.getClass))
  /**
   * We extract the output parameters from t
   * @param t The trial
   * @return one raw in the output table
   */
  def apply(t:ReplicatorTrial):ReplicatorOutput = new ReplicatorOutput(
    turn = t.turn(),
    seedSurvivalChance = if (t.isSeedDominant) 1.0 else 0.0,
    seedResourceAcquisitionFitness = t.seedReplicator.gene.resourceAcquisitionFitness,
    seedResilience =   t.seedReplicator.gene.resilience,
    seedMutationProbability = t.seedReplicator.gene.mutationProbability,
    isFinished = t.isFinished,
    nrOfSeedNodes = t.seedReplicators().size,
    trialUniqueId =t.trialUniqueId
  )

}

/**
 *  Initiates Replicator Experiment
 */
object ReplicatorExperiment {
  val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]
  def main(args : Array[String]): Unit = {
    time {
      val rootGene = Gene(marker = "root")
      val experiment = new Experiment[ReplicatorInput,ReplicatorTrial,ReplicatorOutput](
        name = "Replicator Experiment",
        input = ReplicatorInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 300,

        trialBuilderFunction = trialInput => new ReplicatorTrial(
          maxResource = trialInput.totalResource,
          seedReplicator = new Replicator(
            rootGene.copy(
              resourceAcquisitionFitness = trialInput.seedResourceAcquisitionFitness,
              resilience = trialInput.seedResilience,
              mutationProbability = trialInput.seedMutationProbability,
              mutationFunc =  Gene.mutationFunc0to110UniformRAF,
              ancestor = rootGene,
              marker = "seed", stickyMarker = "seed")),
          nrOfSeedReplicators = 1,
          opponentReplicator
            = new Replicator(rootGene.copy(marker = "opponent", stickyMarker = "opponent", ancestor = rootGene))),
        outputCollectorNeededFunction = trial => trial.turn %1 ==0 || trial.isFinished,
        outputCollectorBuilderFunction = trial => ReplicatorOutput(trial)
      )

      val confidence = 0.99
      import experiment.spark.implicits._
      val inputDimNames = experiment.input.fetchDimensions().mkString(", ")

      experiment.run().toDS().createTempView(ReplicatorOutput.name)
      //val outputDF = experiment.spark.table(ReplicatorOutput.name)
      //log.debug(s"Distinct trials captured : ${outputDF.countTrials()}")
      //log.debug(s"Distinct turns captured : ${outputDF.countTurns()}")


      val sqlSeedSurvivalChance: String = s"select $inputDimNames, count(seedSurvivalChance) as trials, " +
        "avg(seedSurvivalChance) as seedSurvivalChance, " +
        s"error(seedSurvivalChance, $confidence) as error " +
        s"from ${ReplicatorOutput.name}  where isFinished==true group by $inputDimNames order by $inputDimNames"
      log.debug(s"seedSurvivalChanceDF SQL query : $sqlSeedSurvivalChance")

      //println("seedSurvivalChanceDF SQL query : " + sqlSeedSurvivalChance)
      val seedSurvivalChanceDF = experiment.spark.sql(sqlSeedSurvivalChance)
      seedSurvivalChanceDF.show(40)
      val outputFile1 = "output/Replicator_seedSurvivalChance.csv"
      seedSurvivalChanceDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save(outputFile1)
      log.info(s"seedSurvivalChanceDF written to file $outputFile1")

      val prolongTrialsTill = 50 //turns

      experiment.spark.table(ReplicatorOutput.name).retroActivelyProlongTrials(prolongTrialsTill)
        .createTempView(ReplicatorOutput.name +"Prolonged")
      val sqlSeedPopulationByTurn: String = s"select seedResourceAcquisitionFitness, seedMutationProbability, turn, " +
        "avg(nrOfSeedNodes) as seedPopulation, " +
        s"error(nrOfSeedNodes, $confidence) as error " +
        s"from ${ReplicatorOutput.name}Prolonged where turn <= $prolongTrialsTill" +
        s" group by seedResourceAcquisitionFitness,seedMutationProbability, turn  order by seedResourceAcquisitionFitness, seedMutationProbability, turn"
      log.debug(s"seedPopulationByTurnDF SQL query $sqlSeedPopulationByTurn")

      val seedPopulationByTurnDF = experiment.spark.sql(sqlSeedPopulationByTurn)
      seedPopulationByTurnDF.show(100)
      val outputFile2 = "output/Replicator_seedSurvivalChanceByTurn.csv"
      seedPopulationByTurnDF.repartition(1)
        .write.format("csv").option("header","true")
        .mode(SaveMode.Overwrite).save(outputFile2)
      log.info(s"seedPopulationByTurnDF written to file $outputFile2")
      log.info(s"Ending ${experiment.name}")
      experiment.spark.stop()
    }

  }
}