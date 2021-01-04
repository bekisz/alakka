package org.alakka.galtonwatson

import org.montecarlo.Trial

import scala.collection.immutable




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
  def run() :GwTrial = {
    while(!this.isFinished ) {
      this.nextTurn()
    }
    this
  }

}


