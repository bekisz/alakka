package org.montecarlo

trait Trial extends Serializable  {
  def isFinished: Boolean
  def turn():Long
  def nextTurn() : Trial
}
