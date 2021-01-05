package org.montecarlo.examples.galtonwatson

import org.montecarlo.utils

/**
 * One node/agent/person/male family member. All it can do is the spawn children based on Poisson distribution
 * They are always killed at the end of the turn
 * @param lambdaForPoisson
 */
class GwNode(val lambdaForPoisson:Double)  extends Serializable  {


  def createChildren() : List[GwNode] = {
    val numberOfChildren = utils.Statistics.nextRandomDescendantsPoisson(this.lambdaForPoisson)
    List.fill(numberOfChildren)(new GwNode(this.lambdaForPoisson))
  }
}
