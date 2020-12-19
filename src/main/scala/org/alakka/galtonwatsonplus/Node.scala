package org.alakka.galtonwatsonplus

import org.alakka.utils

class Node(val lambdaForPoisson:Double)  extends Serializable  {

  def replicate() = {

  }

  def die() = {

  }

  def mutate() = {

  }

  def tick()  = {

  }
  def createChildren() : List[Node] = {
    val numberOfChildren = utils.Statistics.nextRandomDescendantsPoisson(this.lambdaForPoisson)
    List.fill(numberOfChildren)(new Node(this.lambdaForPoisson))
  }
}
