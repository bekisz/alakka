package org.montecarlo



/**
 * Parameter specifies all possible values of an input parameter.
 * If it has only one element (multiplicity=1), then this parameter acts a constant for the entire experiment
 * Multiple parameter elements triggers the multiplication of trial runs.
 * Example : If the experiment was set run 1000 times, while this parameter has 5 elements then
 * the Monte Carlo Engine triggers not 1000 but 5x1000 runs with all 5 parameters.
 */
case class Parameter[T](elements:IndexedSeq[T]) {
  def head() : T = this.elements.head
  def multiplicity():Int = this.elements.size
}

object Parameter {
  def apply[T](singleValue:T) :Parameter[T] = {
    Parameter[T](Vector[T](singleValue))
  }
  object implicitConversions  {
    implicit def fromIndexedSeq[T](value:IndexedSeq[T]) :Parameter[T] = Parameter[T](value)
    implicit def fromT[T](t:T) : Parameter[T] = Parameter(t)
    implicit def toT[T](parameter:Parameter[T]) : T = parameter.head()
  }

}
