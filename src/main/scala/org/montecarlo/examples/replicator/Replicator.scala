package org.montecarlo.examples.replicator


import org.montecarlo.utils.Statistics

/**
 * One node/agent/person/male family member. All it can do is the spawn children based on Poisson distribution.
 * They are always killed at the end of the turn
 * @param gene The common trait of all instances : The genetic code
 */
class Replicator(var gene:Gene, var resource:Double = 1.0)  extends Serializable  {


  /**
   * Creates children randomly bases on its relative resource acquisition fitness
   * @param resourcePerResourceAcquisitionFitness the number of resource units per 1 resource acquisition fitness unit
   * @return the children and possibly itself
   */
  def nextTurn(resourcePerResourceAcquisitionFitness:Double) : Seq[Replicator] = {

    resource += Statistics.nextRandomPoisson(
      resourcePerResourceAcquisitionFitness * this.gene.resourceAcquisitionFitness )
    resource -= this.gene.resourceConsumptionPerTurn
    this.mutate()
    if (this.resource >= 1)
      this.dieRandomly() ++: (for (_ <- 0 until this.decideNumberOfChildren()) yield this.replicate())
    else Seq.empty[Replicator]
  }
  protected def decideNumberOfChildren() : Int = {
    Math.floor(this.resource).toInt
  }
  protected[this] def mutate():Unit = {
    this.gene =
      if (Math.random() < this.gene.mutationProbability ) this.gene.mutationFunc(this.gene) else this.gene

  }
  protected[this] def dieRandomly(): Option[Replicator]
  = if (Math.random < this.gene.resilience)  Some(this) else None

  def replicate() : Replicator = {
    val passedResource = 1.0
    this.resource -= passedResource
    new Replicator(this.gene, passedResource)
  }

  //override def clone(): Replicator = new Replicator(this.gene)
}
