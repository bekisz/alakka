package org.montecarlo.examples

import ch.qos.logback.classic.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.Seconds
import org.montecarlo.Implicits._
import org.montecarlo.examples.pi.{AggrPiOutput, PiOutput, PiTrial}
import org.montecarlo.{DoubleAccumulatorWithError, Experiment, Input, StreamingConfig}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import scala.math.random

class PiPerformanceTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }
  val piMultiplicity: Long = 100 * 1000L
  val piMaxTurns: Long = 1000
  val trialBuilderFunction: Input => PiTrial = _ => new PiTrial(1000)
  val conf = 0.9999
  val testSparkConf = new SparkConf().setMaster("local[*]")
  private val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]

  test("Classic Spark Way - Pi Approximation") {
    println("Pi Approximation by the classic Spark way")
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .config(testSparkConf)
      .getOrCreate()
    val n: Long = math.min(piMultiplicity * piMaxTurns, Long.MaxValue) // avoid overflow
    //val slices = 4
    val count = spark.sparkContext.parallelize(1L to n).map { _ =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / n} after $n results")
    spark.stop()
  }


  test("With SQL avg and count only - Pi Approximation") {
    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Estimation of Pi by Monte Carlo method",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      sparkConf = testSparkConf
    )
    import experiment.spark.implicits._
    experiment.createOutputRDD().toDF().createTempView(PiOutput.name)
    val aggrOut = experiment.spark
      .sql(s"select count(piValue) as count, avg(piValue) as pi  from ${PiOutput.name}")
      .cache()
    val (myCount, myPi) = (aggrOut.first().getLong(0), aggrOut.first().getDouble(1))
    println(s"The empirical Pi is $myPi")
    println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding $myCount output results with UDAF.")

    experiment.spark.stop()
  }
  test("With 'MvDataFrame.calculateConfidenceInterval' method - Pi Approximation") {

    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Approximation of Pi by Monte Carlo method",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      sparkConf = testSparkConf
    )
    import experiment.spark.implicits._
    val myPiCI = experiment.createOutputRDD().toDF().calculateConfidenceInterval(0.99999)
    println(s"The empirical Pi is ${myPiCI.mean} +/-${myPiCI.mean - myPiCI.low}"
      + s" with ${myPiCI.confidence * 100}% confidence level.")
    println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding ${myPiCI.sampleCount} output results.")

    // We let it fail when 99.999% confidence interval doesn't include the Math.PI
    assert(myPiCI.low < Math.PI && Math.PI < myPiCI.high)
    experiment.spark.stop()

  }

  test("With error UDAF - Pi Approximation") {
    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      sparkConf = testSparkConf
    )
    import experiment.spark.implicits._

    experiment.createOutputRDD().toDF().createOrReplaceTempView(PiOutput.name)
    val out = experiment.spark
      .sql(s"select count(piValue) as count, avg(piValue) as pi, error(piValue, ${conf.toString}) as error"
        + s" from ${PiOutput.name}").as[AggrPiOutput].first()

    println(s"The empirical Pi is ${out.pi} +/-${out.error} with ${conf * 100}% confidence level.")
    println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding ${out.count} output results with UDAF.")

    // We let it fail when 99.999% confidence interval doesn't include the Math.PI
    assert(out.pi - out.error < Math.PI && Math.PI < out.pi + out.error)
    experiment.spark.stop()

  }

  test("With Accumulator and Streaming - Pi Approximation") {
    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      streamingConfig = StreamingConfig(Seconds(3), 100),
      sparkConf = testSparkConf
    )
    val piAccumulator = new DoubleAccumulatorWithError
    experiment.spark.sparkContext.register(piAccumulator, "piAccumulator")
    experiment.foreachRDD(rdd => {
      val dartsProcessed = rdd.map(out => {
        piAccumulator.add(out.piValue); out
      }).count()
      log.info(s"Empirical Pi = ${piAccumulator.avg} +/-${piAccumulator.error(conf)}" +
        s" with ${conf * 100}% confidence level." +
        s"\n       - # of pi estimates received so far (piAccumulator.count) = ${piAccumulator.count}" +
        s"\n       - Latest batch of turns (rdd.count) = $dartsProcessed ")
    })
    experiment.run()
    experiment.spark.stop()
    // We let it fail when 99.9...% confidence interval doesn't include the Math.PI
    assert(piAccumulator.avg - piAccumulator.error(conf) < Math.PI
      && Math.PI < piAccumulator.avg + piAccumulator.error(conf))
  }
  test("With SQL Error UDAF and Streaming - Pi Approximation") {
    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      streamingConfig = StreamingConfig(Seconds(3), 100),
      sparkConf = testSparkConf
    )
    experiment.foreachRDD(rdd => {
      import experiment.spark.implicits._
      rdd.toDF().createOrReplaceTempView(PiOutput.name)
      val out = experiment.spark
        .sql(s"select count(piValue) as count, avg(piValue) as pi, error(piValue, ${conf.toString}) as error"
          + s" from ${PiOutput.name}").as[AggrPiOutput].first()
      log.info(s"The empirical Pi is ${out.pi} +/-${out.error} with ${conf * 100}% confidence level.")
      if (out.count > 100)
        assert(out.pi-out.error < Math.PI && out.pi  < out.pi + out.error)
    })
    experiment.run()
    experiment.spark.stop()
  }
  ignore("With SQL Error UDAF and Cumulative Streaming - Pi Approximation") {
    val experiment = new Experiment[Input, PiTrial, PiOutput](
      name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
      monteCarloMultiplicity = piMultiplicity,
      trialBuilderFunction = trialBuilderFunction,
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0,
      streamingConfig = StreamingConfig(Seconds(3), 100),
      sparkConf = testSparkConf
    )
    var piOutputAllDF:DataFrame = null
    experiment.foreachRDD(rdd => {
      import experiment.spark.implicits._
      piOutputAllDF = if (piOutputAllDF == null)  rdd.toDF() else piOutputAllDF.union(rdd.toDF())

      piOutputAllDF.createOrReplaceTempView(PiOutput.name)
      val out = experiment.spark
        .sql(s"select count(piValue) as count, avg(piValue) as pi, error(piValue, ${conf.toString}) as error"
          + s" from ${PiOutput.name}").as[AggrPiOutput].first()
      log.info(s"The empirical Pi is ${out.pi} +/-${out.error} with ${conf * 100}% confidence level.")
    })
    experiment.run()
  }

}
