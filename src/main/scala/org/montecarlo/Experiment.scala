package org.montecarlo

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
class Experiment[InputType<:Input[InputType]:ClassTag,
  TrialType<:Trial:ClassTag, OutputType:ClassTag](val name:String,
                                         val inputParams: InputType,
                                         val monteCarloMultiplicity:Int,
                                         val trialBuilderFunction : InputType => TrialType,
                                         val outputCollectorBuilderFunction : TrialType => OutputType,
                                         val outputCollectorNeededFunction  : TrialType => Boolean
) extends Serializable with HasMultiplicity {


  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()
  override def multiplicity(): Int = this.inputParams.multiplicity() * this.monteCarloMultiplicity

  def run(): RDD[OutputType] = {
    val inputPermutationsRDD = spark.sparkContext.parallelize(this.inputParams.createInputPermutations())

    print(s"Running ${this.spark.sparkContext.appName} with (")
    print(s"${this.inputParams.fetchParameters().map(_.multiplicity()).mkString("x")}) x")
    print(s" ${this.monteCarloMultiplicity} = ${this.multiplicity()}")

    println(s" trials on Spark " + this.spark.sparkContext.version)
    val outputRDD:RDD[OutputType] = inputPermutationsRDD.flatMap( in => List.fill(this.monteCarloMultiplicity)(in))
    .flatMap (input => {
        val trial = this.trialBuilderFunction(input)
        var outputList = List[OutputType]()
        if (this.outputCollectorNeededFunction(trial))
          outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        while (!trial.isFinished) {
          trial.nextTurn()
          if (this.outputCollectorNeededFunction(trial))
            outputList = this.outputCollectorBuilderFunction(trial) :: outputList
        }
        outputList

    })
    outputRDD
  }

}

