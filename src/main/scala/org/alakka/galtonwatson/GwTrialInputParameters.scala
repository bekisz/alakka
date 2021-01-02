package org.alakka.galtonwatson
import org.montecarlo.Parameter.implicitConversions._
import org.montecarlo.{Parameter, Trial, TrialInputParameters}


case class GwTrialInputParameters(
                                    lambda          : Parameter[Double] = Vector(1.0,1.2,1.5,2.0),
                                    maxPopulation   : Parameter[Long]   = 1000,
                                    override val dataCollectionCondition: Trial=>Boolean = _.isFinished,
                                    override val buildTrial:TrialInputParameters => Trial
                                      = (t) => t match
                                    {
                                      case trialInput:GwTrialInputParameters =>
                                        new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation,
                                        seedNode = new Node(trialInput.lambda ))
                                      //case _ => new RuntimeException("GwTrialInputParameters expected instead of " + t.getClass.toString)
                                    },
                                    override val trialUniqueId    : String = "-- non-unique --"

                     ) extends TrialInputParameters
{
  override def createPermutations() : IndexedSeq[GwTrialInputParameters] =
    for ( oneLambda <- this.lambda.elements; oneMaxPopulation <- maxPopulation.elements)
      yield this.copy(lambda=oneLambda,maxPopulation=oneMaxPopulation)

}
