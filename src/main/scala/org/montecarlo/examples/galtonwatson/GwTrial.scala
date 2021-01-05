package org.montecarlo.examples.galtonwatson

import org.montecarlo.Trial

import scala.collection.immutable


/**
 * One trial in the <A HREF="https://en.wikipedia.org/wiki/Galton%E2%80%93Watson_process">Galton-Watson</A>
 * experiment.
 *
 *
 * @param maxPopulation Dealing with exponential growth while not having unlimited memory and time forces us to
 *                      specify a cutoff population where we can say that the descendant of our
 *                      seed nodes are survived (till eternity)
 * @param seedNode the initial node the is the subject of our enquiry
 */
class GwTrial( val maxPopulation:Long= 100, val seedNode:GwNode = new GwNode(lambdaForPoisson = 1.0))
extends Trial with  Serializable
{
  var livingNodes:immutable.List[GwNode]= seedNode :: immutable.List[GwNode]()

  private var _turn = 0L
  override def turn(): Long = _turn

  private var _isSeedDominant = false


  def isSeedDominant:Boolean = _isSeedDominant

  // private var _isFinished = false
  def isFinished:Boolean = {
    this.livingNodes.isEmpty || this.isSeedDominant
  }
  override def nextTurn() : GwTrial = {
    var nextGenNodes = List[GwNode]()
    for(node <- livingNodes) {
      val children = node.createChildren()
      if (children.size + nextGenNodes.size < maxPopulation)
        nextGenNodes = nextGenNodes ::: children
      else
        this._isSeedDominant = true
    }
    this.livingNodes = nextGenNodes
    _turn +=1
    this
  }
  override def run() :GwTrial = {
    while(!this.isFinished ) {
      this.nextTurn()
    }
    this
  }

}


