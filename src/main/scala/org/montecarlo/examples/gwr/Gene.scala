package org.montecarlo.examples.gwr

/**
 * The blueprint of certain set of nodes
 *
 * @param resourceAcquisitionFitness The capability of acquiring resources from the environment
 * @param ancestor The ancestor gene
 * @param label Label to distinguish this gene
 */
case class Gene(
                 resourceAcquisitionFitness:Double,
                 ancestor:Gene = Gene.root,
                 label:String = ""
               )
object Gene {
  val root:Gene = Gene(1.0, ancestor = null, label = "root")

}

