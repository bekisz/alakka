package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.examples.gw._
import org.montecarlo.examples.gwr._
import org.montecarlo.examples.pi.{PiOutput, PiTrial}
import org.montecarlo.examples.replicator.{Replicator, ReplicatorAnalyzer, ReplicatorInput, ReplicatorOutput, ReplicatorTrial, Gene => RGene}
import org.montecarlo.utils.Time.time
import org.montecarlo.{Analyzer, Experiment, Input}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ExampleTestSuite extends AnyFunSuite with BeforeAndAfter {

  before {

  }
  after {

  }

  test("Pi Estimation Experiment") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        monteCarloMultiplicity = 10000,
        trialBuilderFunction = _ => new PiTrial(100),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = new SparkConf().setMaster("local[*]")
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val myPi = outputDS.select(avg($"piValue").as("piValue")).as[PiOutput].first().piValue
      println(s"Estimated Pi is $myPi after ${outputDS.count()} results in ${experiment.monteCarloMultiplicity} trials.")

      val five9Confidence = Analyzer.calculateConfidenceInterval(outputDS.toDF(), 0.99999)
      // We let it fail when 99.999% confidence interval doesn't include the Math.PI
      assert(five9Confidence.low < Math.PI  && Math.PI < five9Confidence.high)
    }


  }
  test("Galton-Watson Experiment") {

    val experiment = new Experiment[GwInput, GwTrial, GwOutput](
      name = "Galton-Watson Experiment",
      input = GwInput(
        lambda = Vector(1.0, 1.2, 1.5, 2.0), maxPopulation = 1001
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

    println("Confidence Intervals for the survival probabilities")
    Analyzer.calculateConfidenceIntervalsFromGroups(trialOutputDS
      .toDF().withColumn("survivalChance", $"isSeedDominant".cast("Integer"))
      .groupBy("lambda"),
      "survivalChance", List(0.95, 0.99, 0.999)).orderBy("lambda").show()


    analyzer.expectedExtinctionTimesByLambda().show()
    val turns = analyzer.turns()
    val trials = analyzer.trials()
    assert(trials * 5 < turns)
    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")

  }

  test("Galton-Watson Experiment with Resource Limitation ") {

    val experiment = new Experiment[GwrInput, GwrTrial, GwrOutput](
      name = "Galton-Watson with Resources Experiment",
      input = GwrInput(
        seedResourceAcquisitionFitness= Seq(1.0, 1.1, 1.2, 1.5, 2.0, 3.0),
        totalResource = 100L),

      monteCarloMultiplicity = 500,
      trialBuilderFunction = trialInput => new GwrTrial(
        maxResource = trialInput.totalResource,
        seedNode = new GwrNode(Gene(trialInput.seedResourceAcquisitionFitness, label = "seed")),
        nrOfSeedNodes = 1,
        opponentNode = new GwrNode(Gene(1.0, label = "opponent"))
      ),
      outputCollectorNeededFunction = trial => trial.turn % 1 == 0 || trial.isFinished,
      outputCollectorBuilderFunction = trial => GwrOutput(trial),
      sparkConf = new SparkConf().setMaster("local[*]")
    )
    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()
    trialOutputDS.show(20)

    val analyzer = new GwrAnalyzer(trialOutputDS)

    println("Confidence Intervals for the survival probabilities")
    Analyzer.calculateConfidenceIntervalsFromGroups(trialOutputDS
      .toDF().withColumn("survivalChance", $"isSeedDominant".cast("Integer"))
      .groupBy("resourceAcquisitionFitness"),
      "survivalChance", List(0.95, 0.99, 0.999)).orderBy("resourceAcquisitionFitness").show()

    val turns = analyzer.turns()
    val trials = analyzer.trials()
    assert(trials * 5 < turns)

    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")

  }
  test("Replicator Experiment") {
    val experiment = new Experiment[ReplicatorInput,ReplicatorTrial,ReplicatorOutput](
      name = "Galton-Watson with Resources Experiment",
      input = ReplicatorInput(
        seedResourceAcquisitionFitness= Vector(1.0, 1.1, 1.2, 1.5, 2.0, 3.0),
        resilience = Vector(0.0, 0.4, 0.6,0.9, 0.99),
        totalResource = 100L),
      monteCarloMultiplicity =  100,

      trialBuilderFunction = trialInput => new ReplicatorTrial(
        maxResource = trialInput.totalResource,
        seedReplicator = new Replicator(
          RGene(trialInput.seedResourceAcquisitionFitness, trialInput.resilience, label = "seed")),
        nrOfSeedReplicators = 1,
        opponentReplicator = new Replicator(
          RGene(resourceAcquisitionFitness = 1.0, label = "opponent"))),
      outputCollectorNeededFunction = trial => trial.turn %1 ==0 || trial.isFinished,
      outputCollectorBuilderFunction = trial => ReplicatorOutput(trial),
      sparkConf = new SparkConf().setMaster("local[*]")
    )
    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()
    trialOutputDS.show(20)

    val analyzer = new ReplicatorAnalyzer(trialOutputDS)

    println("Confidence Intervals for the survival probabilities")
    Analyzer.calculateConfidenceIntervalsFromGroups(trialOutputDS
      .toDF().withColumn("survivalChance", $"isSeedDominant".cast("Integer"))
      .groupBy("resourceAcquisitionFitness", "resilience"),
      "survivalChance",List(0.99))
      .orderBy("resourceAcquisitionFitness","resilience").show(100)

    val turns = analyzer.turns()
    val trials = analyzer.trials()
    println(s"\n$turns turns processed in $trials trials, averaging "
      + f"${turns.toDouble / trials}%1.1f turns per trial\n")

  }

}
