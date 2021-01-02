package org.montecarlo

trait TrialInputParameters {
  def trialUniqueId : String
  //def dataCollectionStrategy : ComprehensiveDataCollection
  def dataCollectionCondition :Trial => Boolean
  def buildTrial:TrialInputParameters => Trial
  def createPermutations(): IndexedSeq[TrialInputParameters]

}
