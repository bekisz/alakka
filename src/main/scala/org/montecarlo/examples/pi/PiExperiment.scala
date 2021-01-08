package org.montecarlo.examples.pi
import org.apache.spark.sql.functions.avg
import org.montecarlo.utils.Time.time
import org.montecarlo.{Experiment, Input, Trial}
import scala.math.random

/**
 * These fields are the variables that we save from the trial instances for later analysis and will be the columns
 * in our SQL table (Dataset[PiOutput])
 */
case class PiInput() extends Input

class PiTrial extends Trial with  Serializable {
  private var _turn = 0L
  override def turn(): Long = _turn
  private var _isInCircle = false
  def isInCircle:Boolean = _isInCircle
  def isFinished:Boolean = turn()>0
  override def nextTurn() : PiTrial = {
    val x = random * 2 - 1
    val y = random * 2 - 1
    this._isInCircle = x*x + y*y < 1
    _turn +=1
    this
  }
}

case class PiOutput(isInCircle: Boolean)
object PiOutput {
  def apply(t:PiTrial):PiOutput = new PiOutput(t.isInCircle)
}
/**
 * Starting by picking points randomly in a 2x2 box centered in the origin.
 * We estimate PI by the ratio of all points vs points within 1 unit of distance.
 */
object PiExperiment {
  def main(args : Array[String]): Unit = {
    time {
      val experiment = new Experiment[Input,PiTrial,PiOutput](
        name = "Estimation of Pi by Monte Carlo method",
        input = PiInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 10000000,
        trialBuilderFunction = _ => new PiTrial(),
        outputCollectorBuilderFunction =  PiOutput(_),
        outputCollectorNeededFunction = _.isFinished
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val pi = outputDS.select(avg($"isInCircle".cast("Integer"))).first().getAs[Double](0) * 4
      println(s"Estimated Pi is $pi after ${outputDS.count()} trials.")
      experiment.spark.stop()
    }
  }
}