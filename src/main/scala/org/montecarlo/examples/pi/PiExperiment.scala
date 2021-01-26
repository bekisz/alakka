package org.montecarlo.examples.pi
import org.apache.spark.sql.functions.avg
import org.montecarlo.{Analyzer, EmptyInput, Experiment, Trial}
import scala.math.random

/**
 * Picks maxTurns random points in a 2x2 box centered in the origin.
 * If the random point is within 1 unit of distance from origin it sets piValue to 4 otherwise to 0.
 * The average of piValue will provide the estimation of Pi.
 *
 * @param maxTurns Number of turns where in each turn a random point is generated
 */
class PiTrial(val maxTurns: Int = 1) extends Trial with Serializable {
  def isFinished: Boolean = turn() >= this.maxTurns
  var piValue = 0.0
  /**
   * Picks one random point in a 2x2 box centered in the origin.
   * If the random point is within 1 unit of distance from origin it sets _isInCircle to true.
   *
   * @return true if is has a next turn, false it was the final turn for this trial
   */
  override def nextTurn(): Boolean = {
    val (x, y) = (random * 2 - 1, random * 2 - 1)
    piValue = if (x * x + y * y < 1) 4 else 0
    super.nextTurn()
  }
}
case class PiOutput(piValue: Double)
object PiOutput {
  def apply(t: PiTrial): PiOutput = new PiOutput(t.piValue)
}

/**
 * Picks points randomly in a 2x2 box centered in the origin.
 * Pi is estimated by the ratio of points within 1 unit of distance from origin.
 */
object PiExperiment {
  def main(args: Array[String]): Unit = {
    val experiment = new Experiment[EmptyInput, PiTrial, PiOutput](
      name = "Estimation of Pi by the Monte Carlo method",
      monteCarloMultiplicity = if (args.length > 0) args(0).toInt else 1000,
      trialBuilderFunction = _ => new PiTrial(2000),
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0 // we don't need initial pre-run trial outputs
    )
    import experiment.spark.implicits._
    val outputDS = experiment.run().toDS().cache()
    outputDS.show(10)
    val myPi = outputDS.select(avg($"piValue").as("piValue")).as[PiOutput].first().piValue
    val myPiFive9ConfidenceInterval = Analyzer.calculateConfidenceInterval(outputDS.toDF(),0.99999)

    println(s"The estimated Pi is $myPi. The real Pi must be between ${myPiFive9ConfidenceInterval.low}"
      + s"and ${myPiFive9ConfidenceInterval.high} with 99.999% confidence level.")
    println(s"Run ${experiment.monteCarloMultiplicity} trials yielding ${outputDS.count()} output results.")
    experiment.spark.stop()
  }
}