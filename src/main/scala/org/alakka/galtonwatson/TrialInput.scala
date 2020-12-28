package org.alakka.galtonwatson



sealed trait TrialInputType

case class MultiplicityId(trialNo:Int)



case class Dimension[T<:TrialInputType](elements:IndexedSeq[T])
object Dimension {
  def apply[T<:TrialInputType](oneLambda:T) :Dimension[T] = {
    Dimension[T](Vector[T](oneLambda))
  }
}


trait HasLambda extends TrialInputType { val lambda:Double }
trait HasMaxPopulation extends TrialInputType { val maxPopulation:Long }

case class Lambda(lambda: Double) extends HasLambda
case class MaxPopulation(maxPopulation:Long) extends HasMaxPopulation

object DimensionImplicits {
  implicit def Double2Lambda(value : Double):Lambda = Lambda(value)

  implicit def IndexedSeqDouble2LambdaDimension(seqDouble:IndexedSeq[Double]) :Dimension[Lambda]
    = Dimension[Lambda](seqDouble.map(Lambda(_)))

  implicit def Double2LambdaDimension(value : Double):Dimension[Lambda] = Dimension[Lambda](Lambda(value))


  implicit def TrialInputType2Dimension[T<:TrialInputType](value : T):Dimension[T] = Dimension[T](value)

  implicit def IndexedSeq2Dimension[T<:TrialInputType](value:IndexedSeq[T]) :Dimension[T] = Dimension[T](value)

}

import DimensionImplicits._
case class TrialInput(override val lambda: Double, override val maxPopulation: Long, trialNo: Int) extends HasLambda with HasMaxPopulation
class TrialInputRange(
                     val lambdas: Dimension[Lambda] = Vector(1.0,1.2,1.5,2.0),
                     val maxPopulations : Dimension[MaxPopulation] = MaxPopulation(1000)
                     )
