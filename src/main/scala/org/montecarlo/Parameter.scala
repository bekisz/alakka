package org.montecarlo

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}

/**
 * The super class of the fields within the Input subclasses.
 * It can hold one to many inner parameters.  Each inner parameter multiplies the the number of trials executed.
 */
abstract class ParameterBase extends HasMultiplicity {
  /**
   * @return The first inner parameter as Any
   */
  def headAsAny() : Any
  def explode:Seq[ParameterBase]
  /**
   * @return the number of inner parameters
   */
  override def multiplicity():Int

}
/**
 * Parameter specifies all possible values of an input parameter.
 * If it has only one element (multiplicity=1), then this parameter acts a constant for the entire experiment
 * Multiple parameter elements triggers the multiplication of trial runs.
 * Example : If the experiment was set run 1000 times, while this parameter has 5 elements then
 * the Monte Carlo Engine triggers not 1000 but 5x1000 runs with all 5 parameters.
 *
 * A Parameter instance can be constructed from a Seq[T] or by a single T instance with implicit type conversion via
 * Parameter.implicitConversions
 */
case class Parameter[T](elements:Seq[T]) extends ParameterBase {
  /**
   * @return The first inner parameter as T
   */
  def head() : T = this.elements.head

  /**
   * @return The first inner parameter as Any
   */
  override def headAsAny() : Any = this.head()

  /**
   * @return the number of inner parameters
   */
  override def multiplicity():Int = this.elements.size

  /**
   * @return Takes all inner parameters and converts to be be standalone Parameters
   */
  override def explode:Seq[ParameterBase] =
    for( element <- this.elements) yield Parameter[T](Vector(element))
}

object Parameter {
  def apply[T](singleValue:T) :Parameter[T] = {
    Parameter[T](Vector[T](singleValue))
  }

  /**
   * With this you can initialize parameters in a more intuitive way.
   * Input fields can get parameter definitions like
   * <ul>
   * <li>resourceAcquisitionFitness : Parameter[Double] = 1.2</li>
   * <li>resourceAcquisitionFitness : Parameter[Double] = Vector(1.2, 1.5, 2.0)</li>
   * <li>totalResource:Parameter[Long] = 1000 to 5000 by 1000</li>
   * <ul>
   */
  object implicitConversions  {
    implicit def fromIndexedSeq[T](value:Seq[T]) :Parameter[T] = Parameter[T](value)
    implicit def fromT[T](t:T) : Parameter[T] = Parameter(t)
    implicit def toT[T](parameter:Parameter[T]) : T = parameter.head()

  }

}
