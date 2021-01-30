package org.montecarlo.examples.replicator

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
 * @param seedReplicator the initial node the is the subject of our enquiry
 * @param nrOfSeedReplicators The initial number of seed replicators/replicators. seedReplicator is cloned this many times
 * @param opponentReplicator  One opponent node that is runs again our seed node. Its numbers will be filled as long as
 *                      there are available resources
 * @param dominantQualifier When seedReplicators reaches this ratio in the population we consider it a winning position,
 *                          the seed became dominant
 *
 */
class ReplicatorTrial(val maxResource:Long,
                      val seedReplicator:Replicator,
                      val nrOfSeedReplicators:Long,
                      val opponentReplicator:Replicator,
                      val dominantQualifier:Double = 0.8)
  extends Trial with  Serializable
{
  var replicators:Seq[Replicator]= (1 to maxResource.toInt)
    .map{i=> if (i<=nrOfSeedReplicators) seedReplicator.clone() else  opponentReplicator.clone()}

  private var _isSeedDominant = false
  def isSeedDominant:Boolean = _isSeedDominant

  /**
   * Defines when we consider the trial ended. Default : There is only one gene exists
   */
  val isFinishedFunc :Seq[Replicator] => Boolean = nodes => nodes.groupBy(_.gene).size < 2
  def seekDominantGene() : Gene = replicators.groupBy(_.gene).mapValues{_.size}.maxBy{ kv => kv._2}._1
  def seedReplicators() : Seq[Replicator]  = replicators.filter(_.gene == this.seedReplicator.gene)

  /**
   * @return # of all seed descendant replicators (replicators) / all replicators
   */
  def seedRatio() : Double = this.seedReplicators().size.toDouble / this.replicators.size


  def dominantGeneRatio() : Double
  = replicators.groupBy(_.gene).mapValues{_.size}.maxBy{ kv => kv._2}._2.toDouble/ replicators.size

  def isFinished:Boolean =  this.isFinishedFunc(this.replicators)

  /**
   *  One turn where all the replicators given the opportunity to spawn descendants weighted by the relative ratio of their
   *  gene.seedResourceAcquisitionFitness
   *
   *  @return true if is has a next turn, false it was the final turn for this trial
   */
  override def nextTurn() : Boolean = {
    //this.replicators = this.replicators.flatMap(_.dieRandomly())
    val sumOfResourceAcquisitionFitness = replicators.map(_.gene.resourceAcquisitionFitness).sum
    this.replicators = this.replicators.flatMap(_.nextTurn(maxResource/sumOfResourceAcquisitionFitness))
    this._isSeedDominant = this.seedRatio() > this.dominantQualifier
    super.nextTurn()
  }
}


