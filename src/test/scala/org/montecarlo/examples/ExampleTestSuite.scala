package org.montecarlo.examples

import org.apache.spark.SparkConf
import org.montecarlo.Implicits._
import org.montecarlo.examples.gw._
import org.montecarlo.examples.gwr._
import org.montecarlo.examples.pi.{AggrPiOutput, PiOutput, PiTrial}
import org.montecarlo.examples.replicator.{Replicator, ReplicatorAnalyzer, ReplicatorInput, ReplicatorOutput, ReplicatorTrial, Gene => RGene}
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ExampleTestSuite extends AnyFunSuite with BeforeAndAfter {
  val testSparkConf = new SparkConf().setMaster("local[*]")
  before {

  }
  after {

  }

  test("Pi Approximation with calculateConfidenceInterval") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Pi Approximation with calculateConfidenceInterval",
        monteCarloMultiplicity = 10000,
        trialBuilderFunction = _ => new PiTrial(1000),
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
  test("Pi Approximation with User Defined Aggregation Function") {
    time {
      val experiment = new Experiment[Input, PiTrial, PiOutput](
        name = "Pi Approximation with User Defined Aggregation Function",
        monteCarloMultiplicity = 10000,
        trialBuilderFunction = _ => new PiTrial(1000),
        outputCollectorBuilderFunction = PiOutput(_),
        outputCollectorNeededFunction = _.turn() != 0,
        sparkConf = testSparkConf
      )
      val conf = 0.99999
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
      sparkConf = testSparkConf
    )

    val confidence = 0.99
    import experiment.spark.implicits._
    val inputDimNames = experiment.input.fetchDimensions().mkString(", ")

    experiment.run().toDS().createTempView(GwOutput.name)

    val sqlSeedSurvivalChance: String = s"select $inputDimNames, count(seedSurvivalChance) as trials, " +
      "avg(seedSurvivalChance) as seedSurvivalChance, " +
      s"error(seedSurvivalChance, $confidence) as error " +
      s"from ${GwOutput.name}  where isFinished==true group by $inputDimNames order by $inputDimNames"
    println("seedSurvivalChanceDF SQL query : " + sqlSeedSurvivalChance)
    val seedSurvivalChanceDF = experiment.spark.sql(sqlSeedSurvivalChance)
    seedSurvivalChanceDF.show(100)

    println("Multiple Confidence Intervals for the survival probabilities")
    experiment.spark.table(GwOutput.name).toDF()
      .groupBy(experiment.input).calculateConfidenceIntervals(
      "seedSurvivalChance", List(0.95, 0.99, 0.999)).orderBy(experiment.input).show()

    val turns = experiment.spark.table(GwOutput.name).countTurns()
    val trials = experiment.spark.table(GwOutput.name).countTrials()
    println(s"Distinct trials captured : $trials")
    println(s"Distinct turns captured : $turns")

    assert(trials == experiment.multiplicity())
    experiment.spark.stop()
  }

  test("Galton-Watson Experiment with Resource Limitation ") {

    val experiment = new Experiment[GwrInput, GwrTrial, GwrOutput](
      name = "Galton-Watson with Resources Experiment",
      input = GwrInput(
        seedResourceAcquisitionFitness = Seq(1.0, 1.1, 1.2, 1.5, 2.0, 3.0),
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
      sparkConf = testSparkConf
    )
    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()
    trialOutputDS.show(20)



    println("Confidence Intervals for the survival probabilities")
    trialOutputDS.toDF().groupBy(experiment.input).calculateConfidenceIntervals(
      "seedSurvivalChance", List(0.95, 0.99, 0.999)).orderBy(experiment.input).show()

    val turns = experiment.spark.table(GwOutput.name).countTurns()
    val trials = experiment.spark.table(GwOutput.name).countTrials()
    println(s"Distinct trials captured : $trials")
    println(s"Distinct turns captured : $turns")
    assert(trials == experiment.multiplicity())
  }
  test("Replicator Experiment") {
    val experiment = new Experiment[ReplicatorInput, ReplicatorTrial, ReplicatorOutput](
      name = "Galton-Watson with Resources Experiment",
      input = ReplicatorInput(
        seedResourceAcquisitionFitness = Vector(1.0, 1.1, 1.2, 1.5, 2.0, 3.0),
        resilience = Vector(0.0, 0.4, 0.6, 0.9, 0.99),
        totalResource = 100L),
      monteCarloMultiplicity = 100,

      trialBuilderFunction = trialInput => new ReplicatorTrial(
        maxResource = trialInput.totalResource,
        seedReplicator = new Replicator(
          RGene(trialInput.seedResourceAcquisitionFitness, trialInput.resilience, label = "seed")),
        nrOfSeedReplicators = 1,
        opponentReplicator = new Replicator(
          RGene(resourceAcquisitionFitness = 1.0, label = "opponent"))),
      outputCollectorNeededFunction = trial => trial.turn % 1 == 0 || trial.isFinished,
      outputCollectorBuilderFunction = trial => ReplicatorOutput(trial),
      sparkConf = testSparkConf
    )
    import experiment.spark.implicits._
    val trialOutputDS = experiment.run().toDS().cache()
    trialOutputDS.show(20)

    val analyzer = new ReplicatorAnalyzer(trialOutputDS)

    println("Confidence Intervals for the survival probabilities")
    trialOutputDS.toDF()
      .groupBy(experiment.input).calculateConfidenceIntervals(
      "seedSurvivalChance", List(0.99))
      .orderBy(experiment.input).show(100)


    val turns = experiment.spark.table(GwOutput.name).countTurns()
    val trials = experiment.spark.table(GwOutput.name).countTrials()
    println(s"Distinct trials captured : $trials")
    println(s"Distinct turns captured : $turns")
    assert(trials == experiment.multiplicity())

  }

}
