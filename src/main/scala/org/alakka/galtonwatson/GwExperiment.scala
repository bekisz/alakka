package org.alakka.galtonwatson
import org.apache.spark.sql.SparkSession
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Parameter}
import org.montecarlo.Parameter.implicitConversions._

case class GwInput(
                    lambda:Parameter[Double] = Vector(1.2, 1.5, 2.0),
                    maxPopulation:Parameter[Long] = Vector(1000L, 3000L)
                  )

case class GwOutput(turn: Long,
                    isSeedDominant: Boolean,
                    lambda: Double,
                    isFinished: Boolean,
                    nrOfSeedNodes:Int,
                    trialUniqueId:String)

object GwOutput {
  def apply(t:GwTrial):GwOutput =
    GwOutput(
      t.turn(),
      t.isSeedDominant,
      t.seedNode.lambdaForPoisson,
      t.isFinished,
      t.livingNodes.size,
      t.trialUniqueId

    )
}

object GwExperiment {
  def main(args : Array[String]): Unit = {
    time {

      val experiment = new Experiment[GwInput,GwTrial,GwOutput](
        name = "Galton-Watson Experiment",

        inputParams = GwInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 100,
        inputBuilderFunction = {
          case (lambda: Parameter[Double]) :: (maxPopulation: Parameter[Long]) :: Nil => GwInput(lambda, maxPopulation)
        },
        trialBuilderFunction = (uniqueId,trialInput) => new GwTrial(uniqueId, trialInput.maxPopulation,
          seedNode = new GwNode(trialInput.lambda )),
        outputCollectorBuilderFunction = trial => GwOutput(trial),
        outputCollectorNeededFunction = trial => trial.turn() % 5 ==0 || trial.isFinished,
        inputRDDtoDF = inputRDD => {
          val spark: SparkSession = SparkSession.builder.getOrCreate()
          import  spark.implicits._
          inputRDD.toDF()
        }

      )

      val trialOutputRDD = experiment.run().cache()
      import experiment.spark.implicits._
      val trialOutputDS = trialOutputRDD.toDS()
      experiment.generateInputDF()

      val inputOutputDF = experiment.generateInputDF().join(trialOutputDS, "trialUniqueId")
      inputOutputDF.show(20)

      val analyzer = new GwTrialOutputAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(10).show(100)


      analyzer.expectedExtinctionTimesByLambda().show()
      val ticks = analyzer.turns()
      val trials = analyzer.trials()
      println(s"\n$ticks turns processed in $trials trials, averaging "
        + f"${ticks.toDouble / trials}%1.1f turns/t\n")

      experiment.spark.stop()


    }
  }
}