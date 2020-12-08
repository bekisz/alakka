package org.alakka.galtonwatson

import org.alakka.utils

class Node(val lambdaForPoisson:Double)  extends Serializable  {


  def createChildren() : List[Node] = {
    val numberOfChildren = utils.Statistics.nextRandomDescendantsPoisson(this.lambdaForPoisson)
    List.fill(numberOfChildren)(new Node(this.lambdaForPoisson))
  }
}
