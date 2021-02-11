package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.montecarlo.Implicits._
import org.montecarlo.examples.pi.{AggrPiOutput, PiOutput, PiTrial}
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.math.random

class PiPerformanceTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }
  val piMultiplicity:Long = 1*1000*1000L
  val piMaxTurns:Long = 1
  val testSparkConf = new SparkConf().setMaster("local[*]")

  ignore("Classic Spark Way - Pi Approximation") {
    time {
      println("Pi Approximation by the classic Spark way")
      val spark = SparkSession
        .builder
        .appName("Spark Pi")
        .config(testSparkConf)
        .getOrCreate()
      val n:Long = math.min(piMultiplicity * piMaxTurns, Long.MaxValue) // avoid overflow
      //val slices = 4
      val count = spark.sparkContext.parallelize(1L to n).map { _ =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y <= 1) 1 else 0
      }.reduce(_ + _)
      println(s"Pi is roughly ${4.0 * count / n} after $n results")
      spark.stop()
    }
  }

  ignore("With Avg, Count Only - Pi Approximation") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        monteCarloMultiplicity = piMultiplicity,
        trialBuilderFunction = _ => new PiTrial(1000),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = testSparkConf
      )
      import experiment.spark.implicits._
      experiment.run().toDF().createTempView(PiOutput.name)
      val aggrOut = experiment.spark
        .sql(s"select count(piValue) as count, avg(piValue) as pi  from ${PiOutput.name}")
        .cache()
      val (myCount, myPi) = (aggrOut.first().getLong(0), aggrOut.first().getDouble(1))
      println(s"The empirical Pi is $myPi")
      println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding $myCount output results with UDAF.")

      experiment.spark.stop()
    }
  }

  test("With error UDAF - Pi Approximation") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
        monteCarloMultiplicity = piMultiplicity,
        trialBuilderFunction = _ => new PiTrial(1000),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = testSparkConf
      )


      val conf = 0.9999
      import experiment.spark.implicits._

      experiment.run().toDS().createTempView(PiOutput.name)
      val out = experiment.spark
        .sql(s"select count(piValue) as count, avg(piValue) as pi, error(piValue, ${conf.toString}) as error"
          + s" from ${PiOutput.name}").as[AggrPiOutput].first()

      println(s"The empirical Pi is ${out.pi} +/-${out.error} with ${conf*100}% confidence level.")
      println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding ${out.count} output results with UDAF.")

      // We let it fail when 99.999% confidence interval doesn't include the Math.PI
      assert(out.pi - out.error < Math.PI && Math.PI < out.pi + out.error )
      experiment.spark.stop()
    }
  }
  ignore("With 'calculateConfidenceInterval' - Pi Approximation") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Approximation of Pi by Monte Carlo method",
        monteCarloMultiplicity = piMultiplicity,
        trialBuilderFunction = _ =>  new PiTrial(),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = testSparkConf
      )
      import experiment.spark.implicits._
      val myPiCI = experiment.run().toDF().calculateConfidenceInterval(0.99999)
      println(s"The empirical Pi is ${myPiCI.mean} +/-${myPiCI.mean - myPiCI.low}"
        + s" with ${myPiCI.confidence * 100}% confidence level.")
      println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding ${myPiCI.sampleCount} output results.")

      // We let it fail when 99.999% confidence interval doesn't include the Math.PI
      assert(myPiCI.low < Math.PI && Math.PI < myPiCI.high)
      experiment.spark.stop()
    }
  }

}
