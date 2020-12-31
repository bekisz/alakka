package org.alakka.galtonwatson

case class TrialOutput(time: Long,
                       isSeedDominant: Boolean,
                       lambda: Double,
                       isFinished: Boolean,
                       nrOfSeedNodes:Int,
                       trialUniqueId:String
                      )

object TrialOutput {
  def apply(trial:GwTrial):TrialOutput =
    TrialOutput(
      //trial.gwTrialInputParameters,
      trial.time(),
      trial.isSeedDominant,
      trial.seedNode.lambdaForPoisson,
      trial.isFinished,
      trial.livingNodes.size,
      trial.trialUniqueId

    )
}

