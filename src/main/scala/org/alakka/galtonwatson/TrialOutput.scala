package org.alakka.galtonwatson

import org.alakka.utils.ProbabilityWithConfidence

case class TrialOutput(time: Long, isSeedDominant: Boolean, lambda: Double)



object TrialOutput {
  def apply(trial:Trial):TrialOutput = {
    TrialOutput(trial.time(), trial.isSeedDominant(), trial.seedNode.lambdaForPoisson)
  }
}

case class TrialOutputByLambda(lambda:Double,
                               probabilityWithConfidence:ProbabilityWithConfidence,
                               noOfTrials:Long, sumOfTime:Long) {
  override def toString: String =
    s"  - P(survival|lambda=$lambda) = ${probabilityWithConfidence.toString}"
}
