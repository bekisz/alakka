package org.alakka.galtonwatson

import scala.collection.immutable




class Trial(val maxPopulation:Long= 100, val seedNode:Node = new Node(lambdaForPoisson = 1.0))
  extends Serializable
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
  def tick() : Trial = {
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
  def run() :Trial = {
    while(!this.isFinished ) {
      this.tick()
    }
    this
  }

}


