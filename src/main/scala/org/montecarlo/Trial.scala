package org.montecarlo

trait Trial extends Serializable  {
  def isFinished: Boolean
  def time():Long
  def tick() : Trial
}
