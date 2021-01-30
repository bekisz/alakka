package org.montecarlo

import scala.reflect.runtime.universe

/**
 * This trait is to be used as supertype of experiment inputs
 *
 * Its main purpose is to help the Experiment to explode this single experiment input to
 * multiple Trial inputs.
 *
 * Input to an experiment can have parameters each holding
 * multiple values (multiplicity>1), however
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
   * Fetches all the parameter names that has a multiplicity higher than 1 (=dimensions) .
   * This can be a convenient method to anticipate the "groupBy" column names
   *
   * @return paramaters names
   */
  def fetchDimensions() : Seq[String] = {
    val fields = this.getClass.getDeclaredFields
    if (fields.nonEmpty) {
     fields.map(paramField => {
        paramField.setAccessible(true)
        (paramField.getName, paramField.get(this))
      }).collect {
          case (s:String, p: ParameterBase ) if p.multiplicity() > 1 => s
        }

    } else List[String]()

  }

  /**
   * Creates a Map from (field name, BaseParameter)
   * @return
   */
  def fetchParameterMap() :Map[String, ParameterBase] = {
    val fields = this.getClass.getDeclaredFields
    fields.map(paramField => {
      paramField.setAccessible(true)
      (paramField.getName, paramField.get(this))
    }).collect {
      case (s:String, p: ParameterBase ) => (s->p)
    }.toMap
  }
  /**
   * Uses reflection to fetch all the fields within this instance,
   * that are subtype of ParameterBase
    * @return
   */
  def fetchParameters() : Seq[ParameterBase] = {
    val fields = this.getClass.getDeclaredFields
      if (fields.nonEmpty) {
        fields.map(paramField => {
          paramField.setAccessible(true)
          paramField.get(this)
        })
          .collect {
            case p: ParameterBase => p
          }
      } else Seq(Parameter[Int](0))


  }

  /**
   * All possible combinations of Parameters within this instance.
   * In practice it is the product of all the multiplicities of all Parameter[x] fields
   *
   * @return the multiplicity
   */
  override def multiplicity(): Int =  this.fetchParameters().map(_.multiplicity()).product

  /**
   * Creates a new instance of "this" type via reflection
   * This is to the replace the need of adding this builder method in the subtype of this class
   *
   * override def inputBuilder(params:List[ParameterBase]): GwInput = params match {
   *    case (seedResourceAcquisitionFitness: Parameter[Double] ) :: (totalResource: Parameter[Long] ) :: Nil => GwInput (seedResourceAcquisitionFitness, totalResource)
   * }
   *
   * @return The Input Instance
  */
  private[this] def inputBuilder(params:Seq[ParameterBase]): Input = {
    val runtimeMirror: universe.Mirror = universe.runtimeMirror(getClass.getClassLoader)
    val classSymbol = runtimeMirror.staticClass(this.getClass.getName)
    val classMirror = runtimeMirror.reflectClass(classSymbol)
    val primaryConstructorMirror = classMirror.reflectConstructor(classSymbol.primaryConstructor.asMethod)
    primaryConstructorMirror.apply(params:_*).asInstanceOf[Input]
  }

}

/**
 * If the experiement has no other imput than the Monte Carlo multuplicity this empty input can be used
 * out of the box.
 */
case class EmptyInput() extends Input