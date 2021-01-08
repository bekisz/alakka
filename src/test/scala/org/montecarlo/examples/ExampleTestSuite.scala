package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.montecarlo.Experiment
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.examples.galtonwatson._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ExampleTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }
  test("GwExperiment") {

    val experiment = new Experiment[GwInput,GwTrial,GwOutput](
      name = "Galton-Watson Experiment",
      input = GwInput(
          lambda = Vector(1.0, 1.2, 1.5, 2.0), maxPopulation = 1000
      ),
      monteCarloMultiplicity = 500,
      trialBuilderFunction = trialInput => new GwTrial( trialInput.maxPopulation,
        seedNode = new GwNode(trialInput.lambda )),
      outputCollectorBuilderFunction = trial => GwOutput(trial),
      outputCollectorNeededFunction = trial => trial.turn() % 10 ==0 || trial.isFinished,
      sparkConf = new SparkConf().setMaster("local[*]")
    )


    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()

    assert(trialOutputDS.count() >= experiment.multiplicity())
    trialOutputDS.show(20)

    val analyzer = new GwAnalyzer(trialOutputDS)

    analyzer.survivalProbabilityByLambda(confidence = 0.99)
      .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
    analyzer.averagePopulationByLambdaAndTime(10).show(100)


    analyzer.expectedExtinctionTimesByLambda().show()
    val turns = analyzer.turns()
    val trials = analyzer.trials()
    assert(trials*5 < turns)
    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")



  }

}
