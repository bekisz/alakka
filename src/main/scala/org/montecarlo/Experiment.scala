package org.montecarlo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
 * A generic implementation of a Monte-carlo experiment.
 *
 * It starts with a single experiment input case class instance, that specifies all the parameters that trials
 * can just take. As these parameters can have multiple values at this stage, it creates the Cartesian product of these
 * parameters and creates one input for each trials, that are created with the trialBuilderFunction.
 * The outputCollectorBuilderFunction is used to create an OutputType instance that retrieves the needed data from
 * a trial. Data is collected by default by the end of each trial run,  but can be done more frequently (up until
 * turn-by-turn) by changing the outputCollectorNeededFunction
 *
 * @param name The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param input The range of variables for the trials that are executed. Each value will executed by
 *              monteCarloMultiplicity times
 * @param monteCarloMultiplicity The number of trials executed for each potential input variation
 * @param trialBuilderFunction  This the function experiment uses to construct the new trial
 * @param outputCollectorBuilderFunction The function that creates the output collector, the OutputType is the
 *                                       the class that is used in a Dataset or Dataframe later on so its field will
 *                                       act like table columns
 * @param outputCollectorNeededFunction Technically it is possible to take a new OutputType instance after
 *                                                every turn, but that my be just an overkill. If this function is
 *                                                _.isFinished then data will be collected at the end of the trial runs
 *                                                only. If it is (trial=>trial.turn()%10==0 || trial.isFinished)  then...
 */
class Experiment[InputType<:Input:ClassTag,
  TrialType<:Trial:ClassTag, OutputType:ClassTag](val name:String,
                                                  val input: InputType,
                                                  val monteCarloMultiplicity:Int,
                                                  val trialBuilderFunction : InputType => TrialType,
                                                  val outputCollectorBuilderFunction : TrialType => OutputType,
                                                  val outputCollectorNeededFunction  : TrialType => Boolean,
                                                  val sparkConf: SparkConf = new SparkConf()

) extends Serializable with HasMultiplicity {


  val spark: SparkSession = SparkSession.builder.config(sparkConf).appName(name).getOrCreate()

  /**
   * @return the number of Trial runs = input multiplicity x Monte-Carlo multiplicity
   */
  override def multiplicity(): Int = this.input.multiplicity() * this.monteCarloMultiplicity
  override def toString:String =
    s"${this.name} with (" +
    s"${this.input.fetchParameters().map(_.multiplicity()).mkString("x")}) x" +
    s" ${this.monteCarloMultiplicity} = ${this.multiplicity()}" +
    s" trials on Spark ${this.spark.sparkContext.version}"

  protected def welcomeMessage():Unit = println(s"Running ${this.toString()} ...")

  /**
   * Explodes the initial single instance of InputType to feeds them to the Trials created by trialBuilderFunction.
   * Then runs them parallel while collecting the trial output data with the help of OutputType
   * @return the result of the experiment
   */
  def run(): RDD[OutputType] = {

    this.welcomeMessage()
    spark.sparkContext
      .parallelize(this.input.createInputPermutations().asInstanceOf[Seq[InputType]])
      .flatMap( in => List.fill(this.monteCarloMultiplicity)(in))
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
        outputList.reverse

    })
  }

}

