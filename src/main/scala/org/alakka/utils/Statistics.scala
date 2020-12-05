package org.alakka.utils

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

}
