package org.montecarlo

import scala.reflect.ClassTag

trait Output {

  def name:String = getClass.getSimpleName.split('$').head

  protected[this] def  assertInputConformity(inputClass:ClassTag[_]): Unit = {
    if (!this.isInputConform(inputClass)) { throw new RuntimeException("InputConformity check failed")

    }
  }
  def isInputConform(inputClass:ClassTag[_]) :Boolean = {

    val inputFields = inputClass.getClass.getDeclaredFields
    val inputDimensions = if (inputFields.nonEmpty) {
      inputFields.map(paramField => {
        paramField.setAccessible(true)
        (paramField.getName, paramField.get(this))
      }).collect {
        case (s:String, p: ParameterBase ) if p.multiplicity() > 1 => s
      }.toList

    } else List[String]()
    val outputFields = this.getClass.getDeclaredFields
    val outputDimensions = if (outputFields.nonEmpty) {
      outputFields.map(paramField => {
        paramField.setAccessible(true)
        paramField.getName
      }).toList
    } else List[String]()
    inputDimensions.map(s => outputDimensions.contains(s)).reduce(_&_)
  }
}
