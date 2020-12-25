package org.alakka.galtonwatson



sealed trait Dimension

case class MultiplicityId(trialNo:Int)

trait HasLambda extends Dimension { val lambda:Double }

case class Lambda(lambda: Double) extends HasLambda



case class TrialInput(override val lambda: Double, trialNo: Int) extends HasLambda
