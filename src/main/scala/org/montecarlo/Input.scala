package org.montecarlo

trait Input[InputType] extends HasMultiplicity {
  def createInputPermutations():Seq[InputType]  = {

    val allParams = this.fetchParameters().map(_.explode.toList).toList
    org.montecarlo.utils.Collections.cartesianProduct(allParams: _*)
      .map(params => this.inputBuilder(params))

  }

  /**
   * Get all the fields that are are the subtype of ParameterBase
   * @return
   */
  def fetchParameters() : IndexedSeq[ParameterBase] = {
    this.getClass.getDeclaredFields
      .map(paramField => {
        paramField.setAccessible(true)
        paramField.get(this)
      })
      .collect {
        case p: ParameterBase => p
      }

  }
  override def multiplicity(): Int =  this.fetchParameters().map(_.multiplicity()).product
  def inputBuilder(params:List[ParameterBase]): InputType
}
