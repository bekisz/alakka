package org.alakka.utils

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import scala.math.sqrt

object Statistics {
  /**
   * Generates a random number with Poisson distribution
   *
   * Ref : https://stackoverflow.com/questions/1241555/algorithm-to-generate-poisson-and-binomial-random-numbers
   *
   * @param lambda Lambda for Poisson distribution
   * @return
   */
  def nextRandomDescendantsPoisson(lambda:Double) :Int = {
    val l = Math.exp(-lambda)
    var p = 1.0
    var k = 0

    do {
      k += 1
      p *= Math.random
    } while (p > l)
    k-1
  }

  def confidenceInterval(noOfSamples: Long, mean : Double, stdDev : Double, confidence: Double): (Double, Double) = {

    val alpha = 1 - confidence

    /* Student's distribution could be used all the time because it converges
     * towards the normal distribution as n grows.
     */
    if (noOfSamples < 30) {
      (mean - qt(1 - alpha / 2, noOfSamples - 1) * stdDev / sqrt(noOfSamples), mean + qt(1 - alpha / 2, noOfSamples - 1) * stdDev / sqrt(noOfSamples))
    } else {
      (mean - qsnorm(1 - alpha / 2) * stdDev / sqrt(noOfSamples), mean + qsnorm(1 - alpha / 2) * stdDev / sqrt(noOfSamples))
    }

  }

  /** Quantile function for the standard  (μ = 0, σ = 1)  normal distribution
   */
  private def qsnorm(p: Double): Double = {
    new NormalDistribution().inverseCumulativeProbability(p)
  }


  /** Quantile function for the Student's t distribution
   *  Let 0 < p < 1. The p-th quantile of the cumulative distribution function F(x) is defined as
   *  x_p = inf{x : F(x) >= p}
   *  For most of the continuous random variables, x_p is unique and is equal to x_p = F^(-1)(p), where
   *  F^(-1) is the inverse function of F. Thus, x_p is the value for which Pr(X <= x_p) = p. In particular,
   *  the 0.5-th quantile is called the median of F.
   */
  private def qt(p: Double, df: Double): Double = {
    new TDistribution(df).inverseCumulativeProbability(p)
  }


}
