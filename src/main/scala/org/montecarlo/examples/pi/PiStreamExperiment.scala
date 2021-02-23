package org.montecarlo.examples.pi

import ch.qos.logback.classic.Logger
import org.apache.spark.streaming.Seconds
import org.montecarlo.{DoubleAccumulatorWithError, EmptyInput, Experiment}
import org.slf4j.LoggerFactory
/**
 * Picks points randomly in a 2x2 box centered in the origin.
 * Pi is estimated by the ratio of points within 1 unit of distance from origin.
 * This version is implemented with spark streaming so that it is possible to get mid-experiment results
 *
 */
object PiStreamExperiment {
  private val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]

  def main(args: Array[String]): Unit = {

    val experiment = new Experiment[EmptyInput, PiTrial, PiOutput](
      name = "Estimation of Pi by the Monte Carlo method with Streaming",
      monteCarloMultiplicity = if (args.length > 0) args(0).toLong else 100L,
      trialBuilderFunction = _ => new PiTrial(1000),
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0, // we don't need initial pre-run trial outputs
      samplingFrequency = Seconds(1)
    )
    val conf = 0.999
    val piAccumulator = new DoubleAccumulatorWithError
    experiment.spark.sparkContext.register(piAccumulator, "piAccumulator")
    //val outputStream = ssc.queueStream(outputRddQueue, oneAtATime = false)
    experiment.foreachRDD(rdd => {
      rdd.map(x => {piAccumulator.add(x.piValue); x}).collect()
      log.info(s"Empirical Pi = ${piAccumulator.avg} +/-${piAccumulator.error(conf)}" +
        s" with ${conf * 100}% confidence level." +
        s" Darts(sum)=${piAccumulator.count/1000000}M, Darts=${rdd.count()} ")
    })
    experiment.run()
    experiment.spark.stop()
  }

}