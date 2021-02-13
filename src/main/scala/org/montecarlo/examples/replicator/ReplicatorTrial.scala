package org.montecarlo.examples.replicator

import ch.qos.logback.classic.Logger
import org.montecarlo.Trial
import org.slf4j.LoggerFactory


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
 *
 */
class ReplicatorTrial(val maxResource:Long,
                      val seedReplicator:Replicator,
                      val nrOfSeedReplicators:Long,
                      val opponentReplicator:Replicator)
  extends Trial with  Serializable
{
  var replicators:Seq[Replicator]= (1 to maxResource.toInt)
    .map{i=> if (i<=nrOfSeedReplicators) seedReplicator.replicate() else  opponentReplicator.replicate()}
  val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]

  private var _isSeedDominant = false
  private[this] var isTurnStatsDirty = false

  def isSeedDominant:Boolean = _isSeedDominant

  /**
   * Defines when we consider the trial ended. Default : There is only one gene exists
   */
  //val isFinishedFunc :Seq[Replicator] => Boolean = nodes => nodes.map(_.gene.isDescendantOf(
  def seekDominantGene() : Gene = replicators.groupBy(_.gene).mapValues{_.size}.maxBy{ kv => kv._2}._1
  def seedReplicators() : Seq[Replicator]
    = this.replicators.filter(_.gene.stickyMarker.contains(this.seedReplicator.gene.stickyMarker))

  /**
   * @return # of all seed descendant replicators (replicators) / all replicators
   */
  def seedReplicatorRatio() : Double = this.seedReplicators().size.toDouble / this.replicators.size

  def isSeedWon: Boolean =
      this.seedReplicators().size == this.replicators.size
  def isSeedLost: Boolean = this.seedReplicators().isEmpty
  def isFinished: Boolean  = this.isSeedWon || this.isSeedLost

  /**
   *  One turn where all the replicators given the opportunity to spawn descendants weighted by the relative ratio of their
   *  gene.seedResourceAcquisitionFitness
   *
   *  @return true if is has a next turn, false it was the final turn for this trial
   */
  override def nextTurn() : Boolean = {

    this.isTurnStatsDirty = true
    val sumOfResourceAcquisitionFitness = replicators.map(_.gene.resourceAcquisitionFitness).sum
    this.replicators = this.replicators.flatMap(_.nextTurn(maxResource/sumOfResourceAcquisitionFitness))
    log.debug(s"Trial ${this.trialUniqueId} turn: ${this.turn()},"
      + s" seedReplicators Num: ${this.seedReplicators().size}%, "
      + s" seedReplicators %: ${this.seedReplicatorRatio()*100}% ")
    this._isSeedDominant = this.isSeedWon

    super.nextTurn()
  }
}


