package org.alakka.galtonwatson

import org.alakka.utils

class GwNode(val lambdaForPoisson:Double)  extends Serializable  {


  def createChildren() : List[GwNode] = {
    val numberOfChildren = utils.Statistics.nextRandomDescendantsPoisson(this.lambdaForPoisson)
    List.fill(numberOfChildren)(new GwNode(this.lambdaForPoisson))
  }
}
