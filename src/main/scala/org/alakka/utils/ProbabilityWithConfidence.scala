package org.alakka.utils

case class ProbabilityWithConfidence(probability: Double, confidence: Double, low: Double, high: Double) {
  override def toString: String = {

    val str = new StringBuilder(50)
    str ++= "[" ++= f"$low%1.5f" ++= " -> " ++= f"$probability%1.5f" ++= " -> " ++= f"$high%1.5f" + "]"
    str.toString
  }
}
