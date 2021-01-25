package org.montecarlo.examples.replicator


/**
 * The blueprint of certain set of replicators
 *
 * @param resourceAcquisitionFitness The capability of acquiring resources from the environment
 * @param resilience The chance of survival for the next turn  0..1
 * @param ancestor The ancestor gene
 * @param label Label to distinguish this gene
 */
case class Gene(resourceAcquisitionFitness:Double,
                resilience:Double =0.0,
                mutationProbability:Double = 0.999,
                mutationFunc: Gene=>Gene = g=>g,
                ancestor:Gene = Gene.root,
                label:String = ""
               ) extends Cloneable {
  override def clone(): AnyRef = {
    if (Math.random() < mutationProbability ) this.mutationFunc(this)
    else super.clone()
  }
}
object Gene {
  val root:Gene = Gene(resourceAcquisitionFitness = 1.0,resilience = 0.0, ancestor = null, label = "root")
}
