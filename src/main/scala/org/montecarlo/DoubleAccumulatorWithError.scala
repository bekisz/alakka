package org.montecarlo

import org.apache.spark.util.{AccumulatorV2, DoubleAccumulator}
import org.montecarlo.utils.{HasMeasuredLifeTime, Statistics}

/**
 * Same as DoubleAccumulator but collects the sum of squares that is the base standard deviation calculation
 * and standard error calculation
 */
class DoubleAccumulatorWithError(private var _sumOfSquares: Double = 0)
  extends DoubleAccumulator with HasMeasuredLifeTime {

  def error(confidenceLevel: Double): Double = {
    count match {
      case c if c < 3 => Double.MaxValue
      case _ => {
        val mean = this.avg
        val (low, high) = Statistics.confidenceInterval(count, mean, this.stdDev(), confidenceLevel)
        mean - low
      }
    }
  }

  def stdDev(): Double = {
    val mean = this.avg
    Math.sqrt(this.sumOfSquares / this.count - mean * mean)
  }
  def avgSpeedinSecs() : Double = this.count.toDouble / this.lifeTime / 1000

  override def reset(): Unit = {
    _sumOfSquares = 0.0
    super.reset()
  }

  /**
   * Adds v to the accumulator, i.e. increment sumofSquares by v*v, sum by v and count by 1.
   */
  override def add(v: java.lang.Double): Unit = {
    _sumOfSquares += v * v
    super.add(v)
  }

  /**
   * Adds v to the accumulator, i.e. increment sumofSquares by v*v, sum by v and count by 1.
   */
  override def add(v: Double): Unit = {
    _sumOfSquares += v * v
    super.add(v)
  }

  override def copy(): DoubleAccumulatorWithError = {
    val newAccumulator = new DoubleAccumulatorWithError()
    newAccumulator.merge(this)
    newAccumulator
  }

  override def merge(other: AccumulatorV2[java.lang.Double, java.lang.Double]): Unit = other match {
    case a: DoubleAccumulatorWithError =>
      super.merge(a)
      this._sumOfSquares += a.sumOfSquares()
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  def sumOfSquares(): Double = this._sumOfSquares

  override def value: java.lang.Double = super.value

}
