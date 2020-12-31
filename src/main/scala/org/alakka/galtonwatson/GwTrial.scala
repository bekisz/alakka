package org.alakka.galtonwatson

import org.montecarlo.Trial

import scala.collection.immutable




class GwTrial(var trialUniqueId: String, val maxPopulation:Long= 100, val seedNode:Node = new Node(lambdaForPoisson = 1.0))
extends Trial with  Serializable
{

  var livingNodes:immutable.List[Node]= seedNode :: immutable.List[Node]()

  private var _time = 0L
  def time(): Long = _time

  private var _isSeedDominant = false


  def isSeedDominant:Boolean = _isSeedDominant

  // private var _isFinished = false
  def isFinished:Boolean = {
    this.livingNodes.isEmpty || this.isSeedDominant
  }
  def tick() : GwTrial = {
    var nextGenNodes = List[Node]()
    for(node <- livingNodes) {
      val children = node.createChildren()
      if (children.size + nextGenNodes.size < maxPopulation)
        nextGenNodes = nextGenNodes ::: children
      else
        this._isSeedDominant = true
    }
    this.livingNodes = nextGenNodes
    _time +=1
    this
  }
  def run() :GwTrial = {
    while(!this.isFinished ) {
      this.tick()
    }
    this
  }

}


