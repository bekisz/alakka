package org.montecarlo.examples.replicator


import org.montecarlo.utils.Statistics

/**
 * One node/agent/person/male family member. All it can do is the spawn children based on Poisson distribution.
 * They are always killed at the end of the turn
 * @param gene The common trait of all instances : The genetic code
 */
class Replicator(val gene:Gene)  extends Serializable  {
  /**
   * Creates children randomly bases on its relative resource acquisition fitness
   * @param resourcePerResourceAcquisitionFitness the number of resource units per 1 resource acquisition fitness unit
   * @return the children and possibly itself
   */
  def nextTurn(resourcePerResourceAcquisitionFitness:Double) : Seq[Replicator] = {

    val numberOfChildren
      = Statistics.nextRandomPoisson(resourcePerResourceAcquisitionFitness
      *this.gene.resourceAcquisitionFitness)
    this.dieRandomly() ++: List.fill(numberOfChildren)(this.clone())
  }
  protected[this] def dieRandomly(): Option[Replicator]
  = if (Math.random < this.gene.resilience)  Some(this) else None
  override def clone(): Replicator = new Replicator(this.gene)
}
