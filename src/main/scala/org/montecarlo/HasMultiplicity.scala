package org.montecarlo

/**
 * Denotes a class that consists of a set of parameters/trials that can have multiple execution
 */
trait HasMultiplicity {
  def multiplicity(): Int
}
