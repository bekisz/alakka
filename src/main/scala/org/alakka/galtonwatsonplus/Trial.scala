package org.alakka.galtonwatsonplus

import scala.collection.immutable


case class TrialCaseOutput(time:Long, isSeedDominant:Boolean, lambda:Double)

class Trial(val maxPopulation:Long= 100, val seedNode:Node = new Node(lambdaForPoisson = 1.0))
  extends Serializable
{


  var livingNodes:immutable.List[Node]= immutable.List[Node]()

  private var _time = 0L
  def time(): Long = _time

  private var _isSeedDominant = false


  def isSeedDominant():Boolean = _isSeedDominant

  def tick() = {
    var nextGenNodes = List[Node]()
    for(node <- livingNodes) {
      val children = node.createChildren()
      if (children.size + nextGenNodes.size < maxPopulation)
        nextGenNodes = nextGenNodes ::: children
      else
        this._isSeedDominant = true
      _time += 1
    }
    this.livingNodes = nextGenNodes
    this
  }


  def run() :Trial = {
    //logger.info(s"Galton-Watson experiment started with lambda=${this.seedNode.lambdaForPoisson}" )

    livingNodes = seedNode :: livingNodes
    while(this.livingNodes.nonEmpty && !this.isSeedDominant ) {
      this.tick()
    }
    this
  }
  def toCase(): TrialCaseOutput ={
    TrialCaseOutput(time(), isSeedDominant(), this.seedNode.lambdaForPoisson)
  }
}


