package org.montecarlo.examples.gwr

import org.montecarlo.Trial



/**
 * One trial in the <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A>
 * experiment.
 *
 *
 * @param maxResource Dealing with exponential growth while not having unlimited memory and time forces us to
 *                      specify a cutoff population where we can say that the descendant of our
 *                      seed nodes are survived (till eternity)
 * @param seedNode the initial node the is the subject of our enquiry
 */
class GwrTrial(val maxResource:Long,
               val seedNode:GwrNode,
               val nrOfSeedNodes:Long,
               val opponentNode:GwrNode,
               val dominantQualifier:Double = 0.8)
  extends Trial with  Serializable
{
  var nodes:Seq[GwrNode]= (1 to maxResource.toInt)
    .map{i=> if (i<=nrOfSeedNodes) seedNode.clone() else  opponentNode.clone()}
  private var _turn = 0L
  override def turn(): Long = _turn

  private var _isSeedDominant = false
  def isSeedDominant:Boolean = _isSeedDominant
  val isFinishedFunc :Seq[GwrNode] => Boolean = nodes => nodes.groupBy(_.gene).size < 2
  def seekDominantGene() : Gene = nodes.groupBy(_.gene).mapValues{_.size}.maxBy{kv => kv._2}._1
  def seedNodes() : Seq[GwrNode]  = nodes.filter(_.gene == this.seedNode.gene)

  def seedRatio() : Double = this.seedNodes().size.toDouble / this.nodes.size


  def dominantGeneRatio() : Double = nodes.groupBy(_.gene).mapValues{_.size}.maxBy{kv => kv._2}._2.toDouble/ nodes.size


  def isFinished:Boolean =  this.isFinishedFunc(this.nodes)
  override def nextTurn() : GwrTrial = {
    val sumOfResourceAcquisitionFitness = nodes.map(_.gene.resourceAcquisitionFitness).sum
    this.nodes = this.nodes
      .flatMap(node=>node.cloneRandomly(maxResource/sumOfResourceAcquisitionFitness))
    this._isSeedDominant = this.seedRatio() > this.dominantQualifier
    _turn +=1
    this
  }
}


