package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.montecarlo.Implicits._
import org.montecarlo.examples.pi.{PiOutput, PiTrial}
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class PiPerformanceTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }
  val piMultiplicity = 100000

  test("Pi Estimation Experiment") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        monteCarloMultiplicity = piMultiplicity,
        trialBuilderFunction = _ => new PiTrial(1000),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = new SparkConf().setMaster("local[*]")
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val myPi = outputDS.select(avg($"piValue").as("piValue")).as[PiOutput].first().piValue
      val myPiCI = outputDS.toDF().calculateConfidenceInterval(0.99999)
      println(s"The empirical Pi is $myPi +/-${myPi - myPiCI.low}"
        + s" with ${myPiCI.confidence * 100}% confidence level.")
      println(s"Run ${experiment.monteCarloMultiplicity} trials, yielding ${outputDS.count()} output results.")

      // We let it fail when 99.999% confidence interval doesn't include the Math.PI
      assert(myPiCI.low < Math.PI && Math.PI < myPiCI.high)
    }
  }
  test("Pi Estimation Experiment with User Defined Aggregation Function") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Estimation of Pi by Monte Carlo method with User Defined Aggregation Function",
        monteCarloMultiplicity = piMultiplicity,
        trialBuilderFunction = _ => new PiTrial(1000),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = new SparkConf().setMaster("local[*]")
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      outputDS.createTempView("outputDS")
      val df = experiment.spark
        .sql("select avg(piValue) as pi , error(piValue, 0.99999) from outputDS").cache()
      df.show()
      val (myPi, udfError) = (df.first().getAs[Double](0), df.first().getAs[Double](1))
      println(s"The empirical Pi is $myPi +/-$udfError with 99.999% confidence level.")
      println(s"Run ${experiment.monteCarloMultiplicity} trials, " +
        s"yielding ${outputDS.count()} output results with UDAF.")

      // We let it fail when 99.999% confidence interval doesn't include the Math.PI
      assert(myPi - udfError < Math.PI && Math.PI < myPi + udfError)
    }

  }

}
