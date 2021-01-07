package org.montecarlo

import scala.reflect.runtime.universe

/**
 * This trait is to be used as supertype of experiment inputs
 *
 * Its main purpose is to help the Experiment to explode this single experiment input to
 * multiple Trial inputs.
 *
 * Input to an experiment can have parameters each holding
 * multiple values (multiplicty>1), however
 * a single trial accepts input with parameters that has multiplicity of 1.
 *
 * It uses Scala's reflection to discover parameters declared within the subtype.
 * These parameters, in turn, must be a subtype of the ParameterBase class to be considered and the
 * subtype needs to a case class or at least implement its apply method
 *
 */

trait Input extends HasMultiplicity {

  /**
   * Creates all the possible permutations of this Input
   *
   * @return Sequence of Inputs with the element size of multiplicity
   */
  private[montecarlo] def createInputPermutations():Seq[Input]  = {

    val allParams = this.fetchParameters().map(_.explode.toList).toList
    org.montecarlo.utils.Collections.cartesianProduct(allParams: _*)
      .map(params => this.inputBuilder(params))

  }

  /**
   * Uses reflection to fetche all the fields within this instance,
   * that are subtype of ParameterBase
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

  /**
   * All possible combinations of Parameters within this instance.
   * In practice it is the product of all the multiplicities of all Paramater[x] fields
   *
   * @return the multiplicity
   */
  override def multiplicity(): Int =  this.fetchParameters().map(_.multiplicity()).product

  /**
   * Creates a new instance of "this" type via reflection
   * This is to the replace the need of adding this builder method in the subtype of this class
   *
   * override def inputBuilder(params:List[ParameterBase]): GwInput = params match {
   *    case (lambda: Parameter[Double] ) :: (maxPopulation: Parameter[Long] ) :: Nil => GwInput (lambda, maxPopulation)
   * }
   *
   * @return The Input Instance
  */
  private[this] def inputBuilder(params:List[ParameterBase]): Input = {
    val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
    val classSymbol = runtimeMirror.staticClass(this.getClass.getName)
    val classMirror = runtimeMirror.reflectClass(classSymbol)
    val primaryConstructorMirror = classMirror.reflectConstructor(classSymbol.primaryConstructor.asMethod)
    primaryConstructorMirror.apply(params:_*).asInstanceOf[Input]
  }

}