package org.montecarlo

import ch.qos.logback.classic.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.montecarlo.utils.{HasMeasuredLifeTime, Statistics}
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.reflect.ClassTag

/**
 * A generic implementation of a Monte-carlo experiment.
 *
 * It starts with a single experiment input case class instance, that specifies all the parameters that trials
 * can just take. As these parameters can have multiple values at this stage, it creates the Cartesian product of these
 * parameters and creates one input for each trials, that are created with the trialBuilderFunction.
 * The outputCollectorBuilderFunction is used to create an OutputType instance that retrieves the needed data from
 * a trial. Data is collected by default by the end of each trial createOutputRDD,  but can be done more frequently (up until
 * turn-by-turn) by changing the outputCollectorNeededFunction
 *
 * @param name                           The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param input                          The range of variables for the trials that are executed. Each value will executed by
 *                                       monteCarloMultiplicity times
 * @param monteCarloMultiplicity         The number of trials executed for each potential input variation
 * @param trialBuilderFunction           This the function experiment uses to construct the new trial
 * @param outputCollectorBuilderFunction The function that creates the output collector, the OutputType is the
 *                                       the class that is used in a Dataset or Dataframe later on so its field will
 *                                       act like table columns
 * @param outputCollectorNeededFunction  Technically it is possible to take a new OutputType instance after
 *                                       every turn, but that my be just an overkill. If this function is
 *                                       _.isFinished then data will be collected at the end of the trial runs
 *                                       only. If it is (trial=>trial.turn()%10==0 || trial.isFinished)  then...
 */
class Experiment[InputType <: Input : ClassTag,
  TrialType <: Trial : ClassTag, OutputType<:Output: ClassTag](val name: String,
                                                       val input: InputType = EmptyInput(),
                                                       val monteCarloMultiplicity: Long = 1000,
                                                       val trialBuilderFunction: InputType => TrialType,
                                                       val outputCollectorBuilderFunction: TrialType => OutputType,
                                                       val outputCollectorNeededFunction: TrialType => Boolean,
                                                       val sparkConf: SparkConf = new SparkConf(),
                                                       val experimentId:String = UUID.randomUUID().toString
                                                      ) extends Serializable with HasMultiplicity
                                                            with HasMeasuredLifeTime {


  val spark: SparkSession = SparkSession.builder.config(sparkConf).appName(name).getOrCreate()
  private val trialsStartedAccumulator = spark.sparkContext.longAccumulator("trialsStartedAccumulator")
  private val trialTurnsExecutedAccumulator = spark.sparkContext.longAccumulator("trialTurnsExecutedAccumulator")

  val errorUdfFunc : (Double, Double,Double,Double)
    => Double = (weight, sum, sum2, confidence) => {
    //val count = weight
    val mean = sum / weight
    val stdDev = Math.sqrt(sum2 / weight - mean * mean)
    val (low, high) = Statistics.confidenceInterval(weight.toLong, mean, stdDev, confidence)
    mean - low
  }
  this.spark.udf.register("errorUdfFunc", errorUdfFunc)

  this.spark.udf.register("error", new ErrorUDAF)
  this.spark.udf.register("weightedError", new WeightedErrorUDAF)
  this.spark.udf.register("weightedAverage", new WeightedAverageUDAF)

  val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]

  log.info(s"Created ${this.toString} experiment with id ${this.experimentId}...")


  /**
   * Explodes the initial single instance of InputType to feeds them to the Trials created by trialBuilderFunction.
   * Then runs them parallel while collecting the trial output data with the help of OutputType
   *
   * @return the ouput RDD of trial outputs
   */
  def createOutputRDD(monteCarloMultiplicity: Long = this.monteCarloMultiplicity): RDD[OutputType] = {
    val trialInputs = this.input.createInputPermutations().asInstanceOf[Seq[InputType]]
    //val trialInputDS = spark.sparkContext.parallelize(trialInputs)
    spark.sparkContext
      .parallelize(1L to monteCarloMultiplicity).flatMap(_ => trialInputs)
      .map{
        x => this.trialsStartedAccumulator.add(1L)
          x }
      .flatMap{  input => {
        val trial = this.trialBuilderFunction(input)
        var outputList = List[OutputType]()
        do {
          if (this.outputCollectorNeededFunction(trial))
            outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        } while (trial.nextTurn())
        if (this.outputCollectorNeededFunction(trial))
          outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        trialTurnsExecutedAccumulator.add(trial.turn())
        outputList.reverse
      }}
  }


  override def toString: String =
    s"${this.name} with (" +
      s"${this.input.fetchParameters().map(_.multiplicity()).mkString("x")}) x" +
      s" ${this.monteCarloMultiplicity} = ${this.multiplicity()}" +
      s" trials on Spark ${this.spark.sparkContext.version}"

  /**
   * @return the number of Trial runs = input multiplicity x Monte-Carlo multiplicity
   */
  override def multiplicity(): Long = this.input.multiplicity() * this.monteCarloMultiplicity

  def trialsStarted() : Long = this.trialsStartedAccumulator.count
  def trialsExecuted() : Long = this.trialTurnsExecutedAccumulator.count
  def turnsExecuted() : Long = this.trialTurnsExecutedAccumulator.sum
  def avgTrialExecutionSpeedInSecs() : Double
  = this.trialTurnsExecutedAccumulator.count.toDouble / this.lifeTime() / 1000
  def avgTurnExecutionSpeedInSecs() : Double
  = this.trialTurnsExecutedAccumulator.sum.toDouble / this.lifeTime() / 1000


}

