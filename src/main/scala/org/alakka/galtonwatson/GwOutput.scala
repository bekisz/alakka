package org.alakka.galtonwatson

case class GwOutput(turn: Long,
                    isSeedDominant: Boolean,
                    lambda: Double,
                    isFinished: Boolean,
                    nrOfSeedNodes:Int,
                    trialUniqueId:String)

object GwOutput {
  def apply(trial:GwTrial):GwOutput =
    GwOutput(
      trial.turn(),
      trial.isSeedDominant,
      trial.seedNode.lambdaForPoisson,
      trial.isFinished,
      trial.livingNodes.size,
      trial.trialUniqueId

    )
}

