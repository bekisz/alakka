package org.montecarlo.utils

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import scala.math.sqrt

case class ConfidenceInterval(mean: Double, low: Double, high: Double, confidence: Double, sampleCount:Long) {
  override def toString: String = {

    val str = new StringBuilder(50)
    str ++= "[" ++= f"$low%1.5f" ++= " -> " ++=
      f"$mean%1.5f" ++= " -> " ++= f"$high%1.5f" ++= s"] with ${confidence*100} %"
    str.toString
  }
}

object Statistics {
  /**
   * Generates a random number with Poisson distribution
   *
   * <A HREF="https://stackoverflow.com/questions/1241555/algorithm-to-generate-poisson-and-binomial-random-numbers">
   *   Reference</A>
   *
   * @param lambda Lambda for Poisson distribution
   * @return
   */
  def nextRandomPoisson(lambda:Double) :Int = {
    val l = Math.exp(-lambda)
    var p = 1.0
    var k = 0

    do {
      k += 1
      p *= Math.random
    } while (p > l)
    k-1
  }

  /**
   * Approximates the upper and lower bound of real mean within the given confidence level.
   *
   * Caveat : This algorithm approximates the binomiol distribution with Normal distribution.
   * Low p (success) rate and low sample size can give false results.
   * <A HREF="https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation">More on this here</A>
   *
   * @param samples The number of <A HREF="https://en.wikipedia.org/wiki/Bernoulli_trial">Bernoulli Trials
   * @param mean The average of the samples (vs. the real mean what we want to figure out)
   * @param stdDev Standard deviation of the samples
   * @param confidence the requited confidence level ( 1- alpha = signifincance level)
   * @return Lower and upper bound that the true mean is between these values with the requested confidence level
   */
  def confidenceInterval(samples: Long, mean : Double, stdDev : Double, confidence: Double): (Double, Double) = {

    val alpha = 1 - confidence

    /* Student's distribution could be used all the turn because it converges
     * towards the normal distribution as n grows.
     */
    val err =  if (samples < 30)
      new TDistribution(samples - 1).inverseCumulativeProbability(1 - alpha / 2) * stdDev / sqrt(samples)
    else  new NormalDistribution().inverseCumulativeProbability(1 - alpha / 2) * stdDev / sqrt(samples)
    (mean - err, mean + err)
  }
}
