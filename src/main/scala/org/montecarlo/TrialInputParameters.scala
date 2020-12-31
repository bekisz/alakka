package org.montecarlo

abstract class TrialInputParameters {
  val trialUniqueId   : String
  def createPermutations(): IndexedSeq[TrialInputParameters]

}
