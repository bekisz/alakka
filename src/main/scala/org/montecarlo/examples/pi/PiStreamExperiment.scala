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
      name = "Estimation of Pi by the Monte Carlo Method",
      monteCarloMultiplicity = 100*1000,
      trialBuilderFunction = _ => new PiTrial(1000),
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0 // we don't need initial pre-run trial outputs
      //samplingInterval =
    ) {
      override def run(): Unit =  {
        val conf = 0.999
        val piAccumulator = new DoubleAccumulatorWithError
        this.spark.sparkContext.register(piAccumulator, "piAccumulator")

        while(true) {
          val dartsProcessed = this.outputRDD.map(out => {piAccumulator.add(out.piValue); out}).count()
          log.info(s"Empirical Pi = ${piAccumulator.avg} +/-${piAccumulator.error(conf)}" +
            s" with ${conf * 100}% confidence level." +
            s"\n       - # of pi estimates received so far (piAccumulator.count) = ${piAccumulator.count}"+
            s"\n       - Latest batch of turns (rdd.count) = $dartsProcessed ")
        }


      }
    }
    experiment.run()
  }

}