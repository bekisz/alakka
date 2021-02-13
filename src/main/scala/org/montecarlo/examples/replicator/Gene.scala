package org.montecarlo.examples.replicator


/**
 * The blueprint of certain set of replicators
 *
 * @param resourceAcquisitionFitness The capability of acquiring resources from the environment
 * @param resilience                 The chance of survival for the next turn  0..1
 * @param resourceConsumptionPerTurn The amount of resources needed to stay alive in every Turn
 * @param ancestor                   The ancestor gene
 * @param stickyMarker               Label to distinguish this gene and its descendants
 * @param marker                     Label to distinguish this gene
 */

case class Gene(resourceAcquisitionFitness: Double = 1.0,
                resilience: Double = 0.0,
                mutationProbability: Double = 0.0,
                mutationFunc: Gene => Gene = Gene.mutationFuncNone,
                resourceConsumptionPerTurn: Double = 1.0,
                ancestor: Gene = null,
                stickyMarker: String = "",
                marker: String = ""
               ) {

  def isRoot: Boolean = this.ancestor == null

  //@tailrec
  def isDescendantOf(ancestorCandidate: Gene): Boolean = {
    var isDescendant = false
    if (ancestorCandidate == this) isDescendant = true
    else {
      if (!this.isRoot) isDescendant = this.ancestor.isDescendantOf(ancestorCandidate)
    }
    isDescendant
  }

}

object Gene {
  /** Does nothing returns same gene */
  val mutationFuncNone: Gene => Gene = g => g
  /** Creates a different gene but the all else is the same */
  val mutationFuncFlawlessCopy: Gene => Gene = anc => anc.copy(marker = "", ancestor = anc)

  /** Creates a different gene but the all else is the same */
  val mutationFunc0to110UniformRAF: Gene => Gene
  = anc => anc.copy(marker = "", ancestor = anc, resourceAcquisitionFitness = anc.resourceAcquisitionFitness * 1.1 * Math.random())

}