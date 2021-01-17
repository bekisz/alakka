package org.montecarlo.examples.gwr


import org.montecarlo.utils.Statistics

/**
 * One node/agent/person/male family member. All it can do is the spawn children based on Poisson distribution
 * They are always killed at the end of the turn
 * @param gene
 */
class GwrNode(val gene:Gene)  extends Serializable  {
  def cloneRandomly(resourcePerResourceAcquisitionFitness:Double) : Seq[GwrNode] = {
    val numberOfChildren
      = Statistics.nextRandomPoisson(resourcePerResourceAcquisitionFitness *this.gene.resourceAcquisitionFitness)
    //println(s"Children : ${numberOfChildren} res/RAF : ${resourcePerResourceAcquisitionFitness}")
    List.fill(numberOfChildren)(this.clone())
  }

  override def clone(): GwrNode = new GwrNode(this.gene)
}
