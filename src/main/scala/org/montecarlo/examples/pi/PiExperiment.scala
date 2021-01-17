package org.montecarlo.examples.pi
import org.apache.spark.sql.functions.avg
import org.montecarlo.utils.Time.time
import org.montecarlo.{EmptyInput, Experiment, Trial}

import scala.math.random
/**
 *  Picks maxTurns random points in a 2x2 box centered in the origin.
 *  If the random point is within 1 unit of distance from origin it sets _isInCircle to true.
 *
 *  @param maxTurns Number of turns where in each turn a random point is generated
 */

class PiTrial(val maxTurns:Int = 1) extends Trial with  Serializable {

  def isFinished:Boolean = turn()>= this.maxTurns

  private var _isInCircle = false
  def isInCircle:Boolean = _isInCircle

  /**
   *   Picks one random point in a 2x2 box centered in the origin.
   *   If the random point is within 1 unit of distance from origin it sets _isInCircle to true.
   *
   *  @return true if is has a next turn, false it was the final turn for this trial
   */
  override def nextTurn() : Boolean = {
    val x = random * 2 - 1
    val y = random * 2 - 1
    this._isInCircle = x*x + y*y < 1
    super.nextTurn()
  }
}

case class PiOutput(isInCircle: Boolean)
object PiOutput {
  def apply(t:PiTrial):PiOutput = new PiOutput(t.isInCircle)
}
/**
 * Picks points randomly in a 2x2 box centered in the origin.
 * Pi is estimated by the ratio of points within 1 unit of distance from origin.
 */
object PiExperiment {
  def main(args : Array[String]): Unit = {
    time {
      val experiment = new Experiment[EmptyInput,PiTrial,PiOutput](
        name = "Estimation of Pi by the Monte Carlo method",
        input = EmptyInput(),
        monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 1000,
        trialBuilderFunction = _ => new PiTrial(20000),
        outputCollectorBuilderFunction =  PiOutput(_),
        outputCollectorNeededFunction =  _.turn() !=0 // we don't need initial pre-run trial outputs
      )
      import experiment.spark.implicits._
      val outputDS = experiment.run().toDS().cache()
      outputDS.show(10)
      val pi = outputDS.select(avg($"isInCircle".cast("Integer"))).first().getAs[Double](0) * 4
      println(s"Estimated Pi is $pi after ${outputDS.count()} results in ${experiment.monteCarloMultiplicity} trials.")
      experiment.spark.stop()
    }
  }
}