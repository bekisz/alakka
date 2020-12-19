package org.alakka.galtonwatson

trait Dimension
trait HasLambda extends Dimension { val lambda:Double }
case class Lambda(lambda: Double) extends HasLambda
object Lambda {
  def generateAllPossibleValues(): IndexedSeq[Lambda] =
    for (lambda <- BigDecimal(1.0) to BigDecimal(1.6) by BigDecimal(0.1)) yield Lambda(lambda.toDouble)
}

trait HasTrialNo extends Dimension { val trialNo:Int }
case class TrialNo(trialNo:Int) extends HasTrialNo
object TrialNo {
  def generateAllPossibleValues(maxTrialNo:Int = 1000): IndexedSeq[TrialNo]
    = for(n <- 1 to maxTrialNo) yield TrialNo(n)
}

case class TrialInput(override val lambda: Double,override val trialNo: Int) extends HasLambda with HasTrialNo {
  def constructTrial() :Trial =
    new Trial(maxPopulation = 1000, seedNode = new Node(lambda))
}
