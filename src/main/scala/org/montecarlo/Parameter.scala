package org.montecarlo

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
  override def multiplicity():Long

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
  override def multiplicity():Long = this.elements.size

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

}
