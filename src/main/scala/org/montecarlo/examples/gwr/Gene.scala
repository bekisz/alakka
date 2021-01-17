package org.montecarlo.examples.gwr

case class Gene(
                 resourceAcquisitionFitness:Double,
                 ancestor:Gene = Gene.root,
                 label:String = ""
               )
object Gene {
  val root:Gene = Gene(1.0, ancestor = null, label = "root")

}

