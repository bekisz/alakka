package org.montecarlo

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

import scala.collection.mutable
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
                                         val outputCollectorNeededFunction  : TrialType => Boolean,
                                         val inputRDDtoDF:  RDD[(String, InputType)] => DataFrame

) extends Serializable {


  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()
  private[this] val allTrialInputRDD:RDD[(String,InputType)] = this.initAllTrialInputsDS().cache()
  //val flattenedAllTrialInputDF:Dataset[Row] = this.initFlattenedAllTrialInputDF().cache()

  private[this] def createInputPermutations():Seq[InputType]  = {

    val allParams = this.inputParams.getClass.getDeclaredFields
      .map(paramField => {
        paramField.setAccessible(true)
        paramField.get(this.inputParams)
      })
      .map {
        case paramBase: ParameterBase => paramBase.explode.toList
        case other => List.empty[ParameterBase]
      }
      .filter(_.nonEmpty).toList
    org.montecarlo.utils.Collections.cartesianProduct(allParams:_*)
      .map(params => this.inputBuilderFunction(params))

  }

  private[this] def initAllTrialInputsDS(): RDD[(String, InputType)] = {

    import spark.implicits._
    val inputParametersDF = spark.sparkContext.parallelize(this.createInputPermutations())
    val multiplicityDS = (1 to this.monteCarloMultiplicity).toDS()
    multiplicityDS.rdd.cartesian(inputParametersDF).map[(String, InputType)](
      t => (UUID.randomUUID().toString, t._2))
  }

  def generateInputDF() : DataFrame = {


    var newSchema = new StructType()
    newSchema = newSchema.add("trialUniqueId", DataTypes.StringType)
    val rawInputDF= this.inputRDDtoDF(this.allTrialInputRDD)
    val head = rawInputDF.schema.toList(1)

    for (parameterField <- head.dataType.asInstanceOf[StructType]) {

      var typ = parameterField.dataType
      parameterField.dataType match {
        case structType: StructType => typ = structType.head.dataType.asInstanceOf[ArrayType].elementType
        case _ =>
      }
      newSchema = newSchema.add(parameterField.name, typ)
    }
    val newData = rawInputDF
      .collect()
      .map(row =>
      { val anySeq = {
        for( parameters <- row.toSeq(1).asInstanceOf[GenericRowWithSchema].toSeq) yield {
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
        Row.fromSeq(row(0) :: anySeq.toList)
      }).toIndexedSeq
    spark.createDataFrame(spark.sparkContext.parallelize(newData),newSchema)
  }
  def run(): RDD[OutputType] = {

    print(s"Running ${this.spark.sparkContext.appName} with ${this.allTrialInputRDD.count()} trials on ")
    println("Spark " + this.spark.sparkContext.version)
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

