package org.montecarlo.examples.pi

import ch.qos.logback.classic.Logger
import org.apache.spark.sql.DataFrame
import org.montecarlo._
import org.slf4j.LoggerFactory

import scala.math.random

/**
 * Picks maxTurns random points in a 2x2 box centered in the origin.
 * If the random point is within 1 unit of distance from origin it sets sumOf to 4 otherwise to 0.
 * The average of sumOf will provide the estimation of Pi.
 * Rationale : The area of the 2x2 box is 4, the area of the 1 unit radius circle is 1*1*Pi = Pi. =>
 * So Pi/4 == P(inCircle)/1
 *
 * @param maxTurns Number of turns where in each turn a random point is generated
 */
class PiTrial(val maxTurns: Long = 1) extends Trial with Serializable {
  var piValue = 0.0

  def isFinished: Boolean = turn() >= this.maxTurns

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

/**
 * This is will act as one row in our huge table of all Monte Carlo experiment results
 *
 * @param piValue 0 or 4
 */
case class PiOutput(piValue: Double) extends Output

object PiOutput extends Output {
  def apply(t: PiTrial): PiOutput = new PiOutput(t.piValue)
}

/**
 * The case class for final results
 *
 * @param count The number of results yielded by the Monte Carlo experiments
 * @param pi    The empirical pi = mean of [[PiOutput.piValue]]s
 * @param error The sampling error with given confidence level
 */
case class AggrPiOutput(count: Long, pi: Double, error: Double)


/**
 * Picks points randomly in a 2x2 box centered in the origin.
 * Pi is estimated by the ratio of points within 1 unit of distance from origin.
 */
object PiExperiment {
  private val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]

  def main(args: Array[String]): Unit = {

    val experiment = new Experiment[EmptyInput, PiTrial, PiOutput](
      name = "Monte Carlo Pi",
      monteCarloMultiplicity = 100*1000, //Experiment.Infinite,
      trialBuilderFunction = _ => new PiTrial(1000),
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0 // we don't need initial pre-createOutputRDD trial outputs
       //streamingConfig = StreamingConfig(Seconds(3), 100)
    )
    val conf = 0.999
    var piOutputAllDF:DataFrame = null
    experiment.foreachRDD(rdd => {
      import experiment.spark.implicits._
      piOutputAllDF = if (piOutputAllDF == null)  rdd.toDF() else piOutputAllDF.union(rdd.toDF())

      piOutputAllDF.createOrReplaceTempView(PiOutput.name)
      val out = experiment.spark
        .sql(s"select count(piValue) as count, avg(piValue) as pi, error(piValue, ${conf.toString}) as error"
          + s" from ${PiOutput.name}").as[AggrPiOutput].first()
      log.info(s"The empirical Pi is ${out.pi} +/-${out.error} with ${conf * 100}% confidence level.")
    })
    experiment.run()
  }
}