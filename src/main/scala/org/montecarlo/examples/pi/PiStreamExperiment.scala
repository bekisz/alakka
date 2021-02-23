package org.montecarlo.examples.pi

import ch.qos.logback.classic.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.montecarlo.{DoubleAccumulatorWithError, EmptyInput, Experiment}
import org.slf4j.LoggerFactory

import scala.collection.mutable
/**
 * Picks points randomly in a 2x2 box centered in the origin.
 * Pi is estimated by the ratio of points within 1 unit of distance from origin.
 * This version is implemented with spark streaming so that it is possible to get mid-experiment results
 *
 */
object PiStreamExperiment {
  val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]
  //  val exit = false
  var sumPiOutputDF: DataFrame = null

  def main(args: Array[String]): Unit = {

    val experiment = new Experiment[EmptyInput, PiTrial, PiOutput](
      name = "Estimation of Pi by the Monte Carlo method with Streaming",
      monteCarloMultiplicity = if (args.length > 0) args(0).toLong else 10L,
      trialBuilderFunction = _ => new PiTrial(1000),
      outputCollectorBuilderFunction = PiOutput(_),
      outputCollectorNeededFunction = _.turn() != 0 // we don't need initial pre-run trial outputs

    )
    val conf = 0.999
    val outputRddQueue = new mutable.SynchronizedQueue[RDD[PiOutput]]()
    val ssc: StreamingContext = new StreamingContext(experiment.spark.sparkContext, Seconds(5))
    val piAccumulator = new DoubleAccumulatorWithError
    experiment.spark.sparkContext.register(piAccumulator, "piAccumulator")
    val outputStream = ssc.queueStream(outputRddQueue, oneAtATime = false)
    outputStream.foreachRDD(rdd => {
      rdd.map(x => {piAccumulator.add(x.piValue); x}).collect()
      log.info(s"Empirical Pi = ${piAccumulator.avg} +/-${piAccumulator.error(conf)}" +
        s" with ${conf * 100}% confidence level." +
        s" Darts(sum)=${piAccumulator.count/1000000}M, Darts=${rdd.count()} ")

    })
    ssc.start()
    while (true) {
      // println(s"Trials started:  ${experiment.trialsStartedAccumulator.sum}, " +
      //   s"executed:${experiment.trialsExecutedAccumulator.sum}  " )
      outputRddQueue += experiment.createOutputRDD()
    }
    ssc.awaitTermination()
    experiment.spark.stop()
  }

}