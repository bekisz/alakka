package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.montecarlo.{Experiment, Input}
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.examples.galtonwatson._
import org.montecarlo.examples.pi.{PiInput, PiOutput, PiTrial}
import org.montecarlo.utils.Time.time
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ExampleTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }
  test("Galton-Watson Experiment") {

    val experiment = new Experiment[GwInput, GwTrial, GwOutput](
      name = "Galton-Watson Experiment",
      input = GwInput(
        lambda = Vector(1.0, 1.2, 1.5, 2.0), maxPopulation = 1000
      ),
      monteCarloMultiplicity = 500,
      trialBuilderFunction = trialInput => new GwTrial(trialInput.maxPopulation,
        seedNode = new GwNode(trialInput.lambda)),
      outputCollectorBuilderFunction = trial => GwOutput(trial),
      outputCollectorNeededFunction = trial => trial.turn() % 10 == 0 || trial.isFinished,
      sparkConf = new SparkConf().setMaster("local[*]")
    )


    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()

    assert(trialOutputDS.count() >= experiment.multiplicity())
    trialOutputDS.show(20)

    val analyzer = new GwAnalyzer(trialOutputDS)

    analyzer.survivalProbabilityByLambda(confidence = 0.99)
      .collect().foreach(aggregatedOutput => println(aggregatedOutput.toString()))
    analyzer.averagePopulationByLambdaAndTime(10).show(100)


    analyzer.expectedExtinctionTimesByLambda().show()
    val turns = analyzer.turns()
    val trials = analyzer.trials()
    assert(trials * 5 < turns)
    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")

  }
  test("Pi Estimation Experiment") {
    time {
      val experiment = new Experiment[Input,PiTrial,PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        input = PiInput(),
        monteCarloMultiplicity = 1000000,
        trialBuilderFunction = _ => new PiTrial(),
        outputCollectorBuilderFunction =  PiOutput(_),
        outputCollectorNeededFunction = _.isFinished,
        sparkConf = new SparkConf().setMaster("local[*]")
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val pi = outputDS.select(avg($"isInCircle".cast("Integer"))).first().getAs[Double](0) * 4
      println(s"Estimated Pi is $pi after ${outputDS.count()} trials.")
      //experiment.spark.stop()
      assert(pi>3.1 && pi<3.2)
    }


  }
}
