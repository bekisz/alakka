package org.montecarlo.examples.gwr

import org.montecarlo.Trial



/**
 * One trial in a modified  <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A>
 * experiment where the Nodes have Resource Limits and they replicate with the relative weight of the their
 * seedResourceAcquisitionFitness (similar to lambda in the original experiment)
 *
 *
 * @param maxResource Dealing with exponential growth while not having unlimited memory and time forces us to
 *                      specify a cutoff population where we can say that the descendant of our
 *                      seed replicators are survived (till eternity). 1 resource unit can maintain one node (replicator)
 * @param seedNode the initial node the is the subject of our enquiry
 * @param nrOfSeedNodes The initial number of seed replicators/replicators. seedReplicator is cloned this many times
 * @param opponentNode  One opponent node that is runs again our seed node. Its numbers will be filled as long as
 *                      there are available resources
 * @param dominantQualifier When seedReplicators reaches this ratio in the population we consider it a winning position,
 *                          the seed became dominant
 *
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

  private var _isSeedDominant = false
  def isSeedDominant:Boolean = _isSeedDominant

  /**
   * Defines when we consider the trial ended. Default : There is only one gene exists
   */
  val isFinishedFunc :Seq[GwrNode] => Boolean = nodes => nodes.groupBy(_.gene).size < 2
  def seekDominantGene() : Gene = nodes.groupBy(_.gene).mapValues{_.size}.maxBy{kv => kv._2}._1
  def seedNodes() : Seq[GwrNode]  = nodes.filter(_.gene == this.seedNode.gene)

  /**
   * @return # of all seed descendant replicators (replicators) / all replicators
   */
  def seedRatio() : Double = this.seedNodes().size.toDouble / this.nodes.size


  def dominantGeneRatio() : Double = nodes.groupBy(_.gene).mapValues{_.size}.maxBy{kv => kv._2}._2.toDouble/ nodes.size


  def isFinished:Boolean =  this.isFinishedFunc(this.nodes)

  /**
   *  One turn where all the replicators given the opportunity to spawn descendants weighted by the relative ratio of their
   *  gene.seedResourceAcquisitionFitness
   *
   *  @return true if is has a next turn, false it was the final turn for this trial
   */
  override def nextTurn() : Boolean = {
    val sumOfResourceAcquisitionFitness = nodes.map(_.gene.resourceAcquisitionFitness).sum
    this.nodes = this.nodes
      .flatMap(_.cloneRandomly(maxResource/sumOfResourceAcquisitionFitness))
    this._isSeedDominant = this.seedRatio() > this.dominantQualifier
    super.nextTurn()
  }
}


