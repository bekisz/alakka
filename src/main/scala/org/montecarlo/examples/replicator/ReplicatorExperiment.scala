package org.montecarlo.examples.replicator
import ch.qos.logback.classic.Logger
import org.apache.spark.sql.SaveMode
import org.montecarlo.Implicits._
import org.montecarlo.utils.Time.time
import org.montecarlo._
import org.slf4j.LoggerFactory
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

                            seedResourceAcquisitionFitness:Parameter[Double]
                            = ("1.0".toBD to "2.0".toBD by "0.2".toBD).map(_.toDouble),
                            seedResilience:Parameter[Double]
                            =("0.0".toBD to "1.0".toBD by "0.1".toBD).map(_.toDouble),
                            seedMutationProbability:Parameter[Double]
                            =("0.0".toBD to "0.1".toBD by "0.02".toBD).map(_.toDouble),
                            totalResource:Parameter[Long] = 100L
                   ) extends Input

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[ReplicatorOutput])
 */

case class ReplicatorOutput(turn: Long,
                            seedSurvivalChance: Double, // avg
                            seedResourceAcquisitionFitness: Double, // avg
                            seedResilience: Double, //avg
                            seedMutationProbability : Double, //avg
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
  private[this] val databaseUrl:String = "jdbc:postgresql://localhost/replicator?user=spark&password=spark"

  private[this] def save(df:MvDataFrame): Unit = {

    df.write.format("jdbc")
      .option("url", databaseUrl)
      .option("dbtable", df.getName()).mode(SaveMode.Overwrite).save()
    log.info(s"Table ${df.getName()} written to $databaseUrl" )

  }
  private[this] def save2Csv(df:MvDataFrame, directory:String): Unit = {
    val outputFile = s"$directory/${df.getName()}.csv"
    df.repartition(1).write.format("csv").option("header","true")
      .mode(SaveMode.Overwrite).save(outputFile)
    log.info(s"${df.getName()} written to file $outputFile")
  }
  def main(args : Array[String]): Unit = {
    time {
      val rootGene = Gene(marker = "root")
      val experiment = new Experiment[ReplicatorInput,ReplicatorTrial,ReplicatorOutput](
        name = "Replicator",
        input = ReplicatorInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 10,

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
      val inputParamNames = experiment.input.fetchParameterMap().keySet.mkString(", ")

      val prolongTrialsTill = 50 //turns
      experiment.createOutputRDD().toDF().retroActivelyProlongTrials(prolongTrialsTill)
        .createTempView(ReplicatorOutput.name)
      //val outputDF = experiment.spark.table(ReplicatorOutput.tableName)
      //log.debug(s"Distinct trials captured : ${outputDF.countTrials()}")
      //log.debug(s"Distinct turns captured : ${outputDF.countTurns()}")

      val sqlSeedSurvivalChance: String = s"select $inputDimNames, count(seedSurvivalChance) as trials, " +
        "avg(seedSurvivalChance) as seedSurvivalChance, " +
        s"error(seedSurvivalChance, $confidence) as error " +
        s"from ${ReplicatorOutput.name}  where isFinished==true and" +
        s" isProlonged==false group by $inputDimNames order by $inputDimNames"
      log.debug(s"seedSurvivalChanceDF SQL query : $sqlSeedSurvivalChance")

      val seedSurvivalChanceDF = experiment.spark.sql(sqlSeedSurvivalChance).cache().setName("seedSurvivalChance")
      save(seedSurvivalChanceDF)
      save2Csv(seedSurvivalChanceDF, "output/replicator")


      experiment.spark.table(ReplicatorOutput.name).retroActivelyProlongTrials(prolongTrialsTill)
        .createTempView(ReplicatorOutput.name +"Prolonged")
      val sqlSeedPopulationByTurn: String = s"select seedResourceAcquisitionFitness, seedResilience, " +
        s"seedMutationProbability, turn, avg(nrOfSeedNodes) as seedPopulation, " +
        s"error(nrOfSeedNodes, $confidence) as error " +
        s"from ${ReplicatorOutput.name} where turn <= $prolongTrialsTill" +
        s" group by seedResourceAcquisitionFitness, seedResilience, seedMutationProbability, turn  order by " +
        s"seedResourceAcquisitionFitness, seedResilience,seedMutationProbability, turn"
      log.debug(s"seedPopulationByTurnDF SQL query $sqlSeedPopulationByTurn")

      val seedPopulationByTurnDF = experiment.spark.sql(sqlSeedPopulationByTurn).cache().setName("seedPopulationByTurn")
      save(seedPopulationByTurnDF)
      save2Csv(seedPopulationByTurnDF, "output/replicator")

      log.info(s"Ending ${experiment.name} experiment" )
      experiment.spark.stop()
    }

  }
}