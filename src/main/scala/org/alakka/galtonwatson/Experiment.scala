package org.alakka.galtonwatson

import org.alakka.utils.Time.time
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.montecarlo.Parameter

import scala.collection.mutable

/**
 * Implementation of the Galton-Watson experiment by various lambda values and Monte-Carlo trials
 *
 * @param name The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param monteCarloMultiplicity The number of trials executed for each lambda in the lambdaRange Seq
 * @param trialInputParams The range of variables for the trials that are executed. Each value will executed by monteCarloMultiplicity times
 * @param enableInTrialOutputData collection of TrialOutput data  happens after every tick, not only at the end of GwTrial
 *                                Setting it false gives you some performance boost
 */
class Experiment(val name:String,
                 val monteCarloMultiplicity:Int = 1000,
                 val trialInputParams: GwTrialInputParameters,
                 val enableInTrialOutputData:Boolean = true

                ) {


  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()
  val allTrialInputDS:Dataset[GwTrialInputParameters] = this.initAllTrialInputsDS().cache()
  val flattenedAllTrialInputDF:Dataset[Row] = this.initFlattenedAllTrialInputDF().cache()

  private[this] def initAllTrialInputsDS(): Dataset[GwTrialInputParameters] = {

    import spark.implicits._
    val inputParametersDF = this.trialInputParams.createPermutations().toDF()
    val multiplicityDS = (1 to this.monteCarloMultiplicity).toDS()

    multiplicityDS.crossJoin(inputParametersDF)
      .drop("value")
      .withColumn("trialUniqueId", expr("uuid()"))
      .as[GwTrialInputParameters]

  }

  private[this] def initFlattenedAllTrialInputDF() : Dataset[Row] = {


    var newSchema = new StructType()

    for (parameterField <- this.allTrialInputDS.schema.toList) {

      var typ = parameterField.dataType
      parameterField.dataType match {
        case structType: StructType => typ = structType.head.dataType.asInstanceOf[ArrayType].elementType
        case _ =>
      }
      newSchema = newSchema.add(parameterField.name, typ)
    }


    val newData = this.allTrialInputDS.toDF()
      .collect()
      .map(row =>
      { val anySeq = {
        for( parameters <- row.toSeq) yield {
          var yielded = parameters
          parameters match {
            case genericRow:GenericRowWithSchema =>
              genericRow.get(0) match {
                case genericRow0:mutable.IndexedSeq[Any]
                    =>  yielded = genericRow0.head
                case  _ =>
              }
            case  _ =>
          }
          yielded
        }
      }
        Row.fromSeq(anySeq)
      }).toIndexedSeq
    spark.createDataFrame(spark.sparkContext.parallelize(newData),newSchema)

  }
  def run(): Dataset[TrialOutput] = {

    //this.allTrialInputDS = this.initAllTrialInputsDS().cache()

    println(s"${this.spark.sparkContext.appName} started with ${this.allTrialInputDS.count()} trials")
    println("Spark version : " + this.spark.sparkContext.version)

    import Parameter.implicitConversions._
    import spark.implicits._
    val trialOutputDS:Dataset[TrialOutput] = if (this.enableInTrialOutputData) {
      this.allTrialInputDS.flatMap(trialInput => {

          val trial = new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation, seedNode = new Node(trialInput.lambda ))
          var outputList = Seq[TrialOutput]() :+ TrialOutput(trial)
          while (!trial.isFinished) {
            trial.tick()
            outputList = outputList :+ TrialOutput(trial)
          }
          outputList

        })
    } else {
      this.allTrialInputDS.map(trialInput =>
        TrialOutput( new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation, seedNode = new Node(trialInput.lambda)).run()) )
    }
    trialOutputDS
  }

}

object Experiment {

  def main(args : Array[String]): Unit = {

    time {
      val monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 1000
      import Parameter.implicitConversions._
      val inputParameters = GwTrialInputParameters(
        lambda = Vector(1.2, 2.0),
        maxPopulation = 1001
      )
      val experiment = new Experiment("Galton-Watson Experiment", 2, inputParameters)
      println("--experiment.allTrialInputDS")

      experiment.allTrialInputDS.show(100)
      println("--experiment.flattenedAllTrialInputDF")

      experiment.flattenedAllTrialInputDF.show(100)

      val trialOutputDS = experiment.run().cache()

      println("TrialOutputDS")
      trialOutputDS.show(100)
      println("TrialInputOutputDS")
      val inputOutputDF = experiment.flattenedAllTrialInputDF.join(trialOutputDS, "trialUniqueId")
      inputOutputDF.show(100)

      val analyzer = new TrialOutputAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(10).show(100)


      analyzer.expectedExtinctionTimesByLambda().show()
      val ticks = analyzer.ticks()
      val trials = analyzer.trials()
      println(s"\n$ticks ticks (smallest unit of time) processed in $trials trials, averaging "
        + f"${ticks.toDouble / trials}%1.1f ticks/trial\n")

      experiment.spark.stop()


    }
  }
}
