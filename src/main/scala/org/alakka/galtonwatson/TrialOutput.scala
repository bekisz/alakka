package org.alakka.galtonwatson

case class TrialOutput(time: Long, isSeedDominant: Boolean, lambda: Double)

object TrialOutput {
  def apply(trial:Trial):TrialOutput = {
    TrialOutput(trial.time(), trial.isSeedDominant(), trial.seedNode.lambdaForPoisson)
  }
}