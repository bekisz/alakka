package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.montecarlo.examples.galtonwatson.{GwAnalyzer, GwInput, GwNode, GwOutput, GwTrial}
import org.montecarlo.examples.gwr.{Gene, GwrAnalyzer, GwrNode, GwrOutput, GwrTrial, GwrInput}
import org.montecarlo.examples.pi.{PiOutput, PiTrial}
import org.montecarlo.utils.Time.time
import org.montecarlo.{EmptyInput, Experiment, Input}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.montecarlo.Parameter.implicitConversions._

class ExampleTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }

  test("Pi Estimation Experiment") {
    time {
      val experiment = new Experiment[Input,PiTrial,PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        input = EmptyInput(),
        monteCarloMultiplicity = 1000000,
        trialBuilderFunction = _ => new PiTrial(3),
        outputCollectorBuilderFunction =  PiOutput(_),
        outputCollectorNeededFunction =  _.turn() !=0,
        sparkConf = new SparkConf().setMaster("local[*]")
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val pi = outputDS.select(avg($"isInCircle".cast("Integer"))).first().getAs[Double](0) * 4
      println(s"Estimated Pi is $pi after ${outputDS.count()} results in ${experiment.monteCarloMultiplicity} trials.")

      //experiment.spark.stop()
      assert(pi>3.1 && pi<3.2)
    }


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

  test("Galton-Watson Experiment with Resource Limitation ") {

    val experiment = new Experiment[GwrInput,GwrTrial,GwrOutput](
      name = "Galton-Watson with Resources Experiment",
      input = GwrInput(),
      monteCarloMultiplicity = 1000,
      trialBuilderFunction = trialInput => new GwrTrial(
        maxResource = trialInput.totalResource,
        seedNode = new GwrNode(Gene(trialInput.seedResourceAcquisitionFitness, label = "seed")),
        nrOfSeedNodes = 1,
        opponentNode = new GwrNode(Gene(1.0, label = "opponent"))
      ),
      outputCollectorNeededFunction = trial => trial.turn %1 ==0 || trial.isFinished,
      outputCollectorBuilderFunction = trial => GwrOutput(trial),
      sparkConf = new SparkConf().setMaster("local[*]")
    )
    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()
    trialOutputDS.show(20)

    val analyzer = new GwrAnalyzer(trialOutputDS)

    analyzer.survivalProbabilityByLambda(confidence = 0.99)
      .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
    analyzer.averagePopulationByLambdaAndTime(30).show(100)


    //analyzer.expectedExtinctionTimesByLambda().show()
    val turns = analyzer.turns()
    val trials = analyzer.trials()
    assert(trials * 5 < turns)

    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")

  }
}
