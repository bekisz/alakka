package org.montecarlo

import ch.qos.logback.classic.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}
import org.montecarlo.utils.{HasMeasuredLifeTime, Statistics, Time}
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.collection.mutable
import scala.reflect.ClassTag

case class StreamingConfig(samplingInterval: Duration, monteCarloMultiplicityPerBatches: Long)

object StreamingConfig {
  val noStreaming: StreamingConfig = StreamingConfig(Milliseconds(Long.MaxValue), 0L)
}

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
  TrialType <: Trial : ClassTag, OutputType <: Output : ClassTag]
(val name: String,
 val input: InputType = EmptyInput(),
 val monteCarloMultiplicity: Long = 1000,
 val streamingConfig: StreamingConfig = StreamingConfig.noStreaming,
 val trialBuilderFunction: InputType => TrialType,
 val outputCollectorBuilderFunction: TrialType => OutputType,
 val outputCollectorNeededFunction: TrialType => Boolean,
 val sparkConf: SparkConf = new SparkConf(),
 val experimentId: String = UUID.randomUUID().toString
) extends HasMultiplicity with HasMeasuredLifeTime {

  if (this.streamingConfig == StreamingConfig.noStreaming && this.monteCarloMultiplicity == Experiment.Infinite)
    throw new IllegalArgumentException("Non-streaming experiment with infinite monteCarloMultiplicity never ends")
  val spark: SparkSession = SparkSession.builder.config(sparkConf).appName(name).getOrCreate()
  private val outputRddQueue = new mutable.SynchronizedQueue[RDD[OutputType]]()
  private val ssc: StreamingContext
  = new StreamingContext(this.spark.sparkContext, streamingConfig.samplingInterval)
  private val outputStream = ssc.queueStream(outputRddQueue, oneAtATime = false)

  private val errorUdfFunc: (Double, Double, Double, Double)
    => Double = (weight, sum, sum2, confidence) => {
    val mean = sum / weight
    val stdDev = Math.sqrt(sum2 / weight - mean * mean)
    val (low, _) = Statistics.confidenceInterval(weight.toLong, mean, stdDev, confidence)
    mean - low
  }
  private val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]
  private val trialsStartedAccumulator = spark.sparkContext.longAccumulator("trialsStartedAccumulator")
  this.spark.udf.register("errorUdfFunc", errorUdfFunc)

  this.spark.udf.register("error", new ErrorUDAF)
  this.spark.udf.register("weightedError", new WeightedErrorUDAF)
  this.spark.udf.register("weightedAverage", new WeightedAverageUDAF)
  private val trialTurnsExecutedAccumulator = spark.sparkContext.longAccumulator("trialTurnsExecutedAccumulator")

  log.info(s"Created ${this.toString} experiment with id ${this.experimentId}...")
  log.debug(this.describe())

  var rddActionFunction: RDD[OutputType] => Unit
  = { _ =>
    throw new UnsupportedOperationException(
      "Experiment run was called before specifying and RDD Action function with foreachRDD")
  }

  def foreachRDD(rddActionFunction: RDD[OutputType] => Unit): Unit = {
    this.rddActionFunction = rdd=>  rddActionFunction(rdd)
    if (this.streamingConfig != StreamingConfig.noStreaming) { // Spark Streaming Solution
      this.outputStream.foreachRDD(this.rddActionFunction)
    }
  }

  def run(): Unit = {
    if (this.streamingConfig != StreamingConfig.noStreaming) { // Spark Streaming Solution
      ssc.start()
      if (this.multiplicity() == Experiment.Infinite)
        while (true)
          outputRddQueue += this.createOutputRDD(streamingConfig.monteCarloMultiplicityPerBatches)
      else {
        for (_ <- this.multiplicity() to 1 by -this.streamingConfig.monteCarloMultiplicityPerBatches)
          outputRddQueue += this.createOutputRDD(streamingConfig.monteCarloMultiplicityPerBatches)
        outputRddQueue += this.createOutputRDD(this.multiplicity()
          % streamingConfig.monteCarloMultiplicityPerBatches)
      }
      log.debug(s"Created OutputRDD(s)")
      ssc.stop(false, true)

    } else this.rddActionFunction(this.createOutputRDD()) // Traditional one big RDD solution
    log.info(s"Experiment finished after ${Time.durationFromMillisToHumanReadable(this.lifeTime())}"
      + s"\n        - Total trials executed = ${trialsExecuted()}"
      + s"\n        - Velocity = " + f"${this.avgTrialExecutionSpeedInSecs()}%1.3f trials/s" +
      f" = ${this.avgTurnExecutionSpeedInSecs()}%1.0f turns/s"
    )
  }

  /**
   * Explodes the initial single instance of InputType to feeds them to the Trials created by trialBuilderFunction.
   * Then runs them parallel while collecting the trial output data with the help of OutputType
   *
   * @return the output RDD of trial outputs
   */
  def createOutputRDD(monteCarloMultiplicity: Long = this.monteCarloMultiplicity): RDD[OutputType] = {
    val trialInputs = this.input.createInputPermutations().asInstanceOf[Seq[InputType]]
    val thisOutputCollectorBuilderFunction = this.outputCollectorBuilderFunction
    val thisOutputCollectorNeededFunction = this.outputCollectorNeededFunction
    val thisTrialBuilderFunction = this.trialBuilderFunction
    val thisTrialsStartedAccumulator = this.trialsStartedAccumulator
    val thisTrialTurnsExecutedAccumulator = this.trialTurnsExecutedAccumulator
    spark.sparkContext
      .parallelize(1L to monteCarloMultiplicity).flatMap(_ => trialInputs)
      .map {
        x =>
          thisTrialsStartedAccumulator.add(1L)
          x
      }
      .flatMap { input => {
        val trial = thisTrialBuilderFunction(input)
        var outputList = List[OutputType]()
        do {
          if (thisOutputCollectorNeededFunction(trial))
            outputList = thisOutputCollectorBuilderFunction(trial) :: outputList
        } while (trial.nextTurn())
        if (thisOutputCollectorNeededFunction(trial))
          outputList = thisOutputCollectorBuilderFunction(trial) :: outputList
        thisTrialTurnsExecutedAccumulator.add(trial.turn())
        outputList.reverse
      }
      }
  }

  def trialsExecuted(): Long = this.trialTurnsExecutedAccumulator.count

  def avgTrialExecutionSpeedInSecs(): Double
  = this.trialTurnsExecutedAccumulator.count.toDouble / this.lifeTime() * 1000

  def avgTurnExecutionSpeedInSecs(): Double
  = this.trialTurnsExecutedAccumulator.sum.toDouble / this.lifeTime() * 1000

  /**
   * Utility function to for fetching all debug info on this experiment
   * @return
   */
  def describe(): String = {
    val monteCarloMultiplicity = if (this.monteCarloMultiplicity == Experiment.Infinite) "Infinite"
    else this.monteCarloMultiplicity.toString
    val multiplicity = if (this.multiplicity() == Experiment.Infinite) "Infinite"
    else this.multiplicity().toString
    val usingStreaming = if (this.streamingConfig == StreamingConfig.noStreaming) "no" else "yes"
    s"\n - Experiment name         : ${this.name}" +
      s"\n - Experiment id           : ${this.experimentId}" +
      s"\n - Input Cardinality       : ${this.input.fetchParameters().map(_.multiplicity()).mkString("x")}" +
      s"\n - Monte Carlo cardinality : $monteCarloMultiplicity" +
      s"\n - Experiment cardinality  : $multiplicity" +
      s"\n - Streaming               : $usingStreaming" + {
      if (this.streamingConfig != StreamingConfig.noStreaming) {
        s"\n    - Sampling Interval         : ${streamingConfig.samplingInterval}" +
          s"\n    - Monte Carlo cardinality per batch : ${streamingConfig.monteCarloMultiplicityPerBatches}"
      } else ""
    } +
      s"\n - Spark version           : ${this.spark.sparkContext.version}"
  }

  /**
   * @return the number of Trial runs = input multiplicity x Monte-Carlo multiplicity
   */
  override def multiplicity(): Long
  = if (this.monteCarloMultiplicity == Experiment.Infinite) Experiment.Infinite
  else this.input.multiplicity() * this.monteCarloMultiplicity

  override def toString: String = this.name

  def trialsStarted(): Long = this.trialsStartedAccumulator.count

  def turnsExecuted(): Long = this.trialTurnsExecutedAccumulator.sum


}

object Experiment {
  val Infinite: Long = Long.MaxValue
}