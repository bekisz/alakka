package org.montecarlo

import java.util.UUID

import org.alakka.galtonwatson._
import org.alakka.utils.Time.time
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

/**
 * Implementation of the Galton-Watson experiment by various lambda values and Monte-Carlo trials
 *
 * @param name The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param monteCarloMultiplicity The number of trials executed for each lambda in the lambdaRange Seq
 * @param inputParams The range of variables for the trials that are executed. Each value will executed by monteCarloMultiplicity times
 *
 */
class Experiment[InputType:ClassTag,
  TrialType<:Trial, OutputType:ClassTag](val name:String,
                                         val inputParams: InputType,
                                         val monteCarloMultiplicity:Int,
                                         val inputBuilderFunction :List[ParameterBase] => InputType,
                                         val trialBuilderFunction
                                                : (String, InputType) => TrialType,
                                         val outputCollectorBuilderFunction : TrialType => OutputType,
                                         val outputCollectorNeededFunction  : TrialType => Boolean
) extends Serializable {


  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()
  val allTrialInputRDD:RDD[(String,InputType)] = this.initAllTrialInputsDS().cache()
  //val flattenedAllTrialInputDF:Dataset[Row] = this.initFlattenedAllTrialInputDF().cache()
/*
  def toStringWithFields: String = {
    val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(c))
    }

    s"${c.getClass.getName}(${fields.mkString(", ")})"
  }*/
  private[this] def createInputPermutations():Seq[InputType]  = {

    val allParams = this.inputParams.getClass.getDeclaredFields.map(paramField=> {
      //println("Parameter Name : " +paramField.getName)
      //println("Parameter Type : " +paramField.getType.toString)
      paramField.setAccessible(true)
      val paramBase = paramField.get(inputParams).asInstanceOf[ParameterBase]
      val explodedParams = paramBase.explode

      //paramRef.getClass.getField("elements").
      //println("Parameter Value : " +paramField.get(inputParams))
      //f.get(inputParams).getClass.getDeclaredField("elements").get(f.get(inputParams)))
      explodedParams.toList
    }).toList

    org.alakka.utils.Collections.cartesianProduct(allParams:_*)
      .map(params => this.inputBuilderFunction(params))

  }

  private[this] def initAllTrialInputsDS(): RDD[(String, InputType)] = {

    import spark.implicits._
    val inputParametersDF = spark.sparkContext.parallelize(this.createInputPermutations())
    val multiplicityDS = (1 to this.monteCarloMultiplicity).toDS()
    multiplicityDS.rdd.cartesian(inputParametersDF).map[(String, InputType)](
      t => (UUID.randomUUID().toString, t._2))
  }
  /*
  def getFlattenedAllTrialInputDF() : Dataset[Row] = {


    var newSchema = new StructType()
    import this.spark.implicits._
    println("Showing allTrialInputRDD to DS")
    this.allTrialInputRDD.toDS().show(10)

    for (parameterField <- this.allTrialInputRDD.toDF().schema.toList) {

      var typ = parameterField.dataType
      parameterField.dataType match {
        case structType: StructType => typ = structType.head.dataType.asInstanceOf[ArrayType].elementType
        case _ =>
      }
      newSchema = newSchema.add(parameterField.name, typ)
    }


    val newData = this.allTrialInputRDD.toDF()
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

  } */
  def run(): RDD[OutputType] = {

   // println(s"Running ${this.spark.sparkContext.appName} with ${this.allTrialInputRDD.count()} trials")
    println("Spark version : " + this.spark.sparkContext.version)
    val outputRDD:RDD[OutputType] = this.allTrialInputRDD.flatMap {
      case (uniqueId: String, trialInput: InputType) =>

        val trial = this.trialBuilderFunction(uniqueId, trialInput)
        var outputList = List[OutputType]()
        if (this.outputCollectorNeededFunction(trial))
          outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        while (!trial.isFinished) {
          trial.nextTurn()
          if (this.outputCollectorNeededFunction(trial))
            outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        }
        outputList

    }
    outputRDD
  }

}

object Experiment {
  def main(args : Array[String]): Unit = {

    time {
      import Parameter.implicitConversions._



      val experiment = new Experiment[GwInput,GwTrial,GwOutput](
        name = "Galton-Watson Experiment",
        inputParams = GwInput(
          lambda = Vector(1.2, 1.5, 2.0),
          maxPopulation = Vector(1000L,3000L)
        ),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 100,
        inputBuilderFunction = {
          case (lambda: Parameter[Double]) :: (maxPopulation: Parameter[Long]) :: Nil => GwInput(lambda, maxPopulation)
        },
        trialBuilderFunction = (uniqueId,trialInput) => new GwTrial(uniqueId, trialInput.maxPopulation,
          seedNode = new GwNode(trialInput.lambda )),
        outputCollectorBuilderFunction = trial => GwOutput(trial),
        outputCollectorNeededFunction = trial => trial.turn() % 5 ==0 || trial.isFinished

      )

      //println("--experiment.allTrialInputRDD")

      //experiment.allTrialInputRDD.show(100)
      //println("--experiment.flattenedAllTrialInputDF")

      //experiment.flattenedAllTrialInputDF.show(100)

      val trialOutputRDD = experiment.run().cache()
      //experiment.getFlattenedAllTrialInputDF()
      import experiment.spark.implicits._
      val trialOutputDS = trialOutputRDD.toDS()
      println("TrialOutputDS")
      trialOutputDS.show(100)
      //println("TrialInputOutputDS")
      //val inputOutputDF = experiment.flattenedAllTrialInputDF.join(trialOutputDS, "trialUniqueId")
      //inputOutputDF.show(100)

      val analyzer = new TrialOutputAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(10).show(100)


      analyzer.expectedExtinctionTimesByLambda().show()
      val ticks = analyzer.ticks()
      val trials = analyzer.trials()
      println(s"\n$ticks ticks (smallest unit of turn) processed in $trials trials, averaging "
        + f"${ticks.toDouble / trials}%1.1f ticks/trial\n")

      experiment.spark.stop()


    }
  }
}
