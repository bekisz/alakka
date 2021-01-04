package org.montecarlo

import java.util.UUID

trait Trial extends Serializable  {
  def isFinished: Boolean
  def turn():Long
  def nextTurn() : Trial
  val trialUniqueId = UUID.randomUUID().toString
}
