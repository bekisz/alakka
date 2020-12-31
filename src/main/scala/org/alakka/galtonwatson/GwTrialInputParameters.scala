package org.alakka.galtonwatson
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.{Parameter, TrialInputParameters}


case class GwTrialInputParameters(
                                    lambda          : Parameter[Double] = Vector(1.0,1.2,1.5,2.0),
                                    maxPopulation   : Parameter[Long]   = 1000,
                                    override val trialUniqueId : String = "-- non-unique --"
                     ) extends TrialInputParameters
{
  override def createPermutations() : IndexedSeq[GwTrialInputParameters] =
    for ( oneLambda <- this.lambda.elements; oneMaxPopulation <- maxPopulation.elements)
      yield GwTrialInputParameters(oneLambda, oneMaxPopulation)
}
