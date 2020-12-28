package org.alakka.galtonwatson

import org.alakka.utils.ProbabilityWithConfidence

case class TrialOutput(time: Long, isSeedDominant: Boolean, lambda: Double, isFinished: Boolean, nrOfSeedNodes:Int)



object TrialOutput {
  def apply(trial:Trial):TrialOutput = {
    TrialOutput(trial.time(), trial.isSeedDominant, trial.seedNode.lambdaForPoisson, trial.isFinished, trial.livingNodes.size)
  }
}

