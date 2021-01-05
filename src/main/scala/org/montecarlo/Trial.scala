package org.montecarlo

import java.util.UUID

/**
 * Common trait of all trials. Trials can be driven turn-by-turn till it gets to an isFinished=true state
 * or can be started by run where the control is given back to the framework after the trial is fully finished with
 * all of its turns.
 *
 * All Trials subclasses inherit an universal unique ID
 */
trait Trial extends Serializable  {
  def isFinished: Boolean

  /**
   * Runs the trial with 1-many turns till it gets finished
   * @return this trial
   */
  def run():Trial

  /**
   * @return turn number ~ time
   */
  def turn():Long

  /**
   * Takes one turn
   * @return this trial
   */
  def nextTurn() : Trial

  /**
   * The universally unique id of this trial
   */
  val trialUniqueId = UUID.randomUUID().toString
}
