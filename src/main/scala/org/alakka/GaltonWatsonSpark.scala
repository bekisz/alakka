package org.alakka

import org.alakka.utils.Time.time
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalameter.utils.Statistics

import scala.collection.immutable

case class ProbabilityWithConfidence(probability:Double,confidence:Double, low:Double, high:Double) {
  override def toString:String = {

    val str = new StringBuilder(50)
    str++= "["++=f"$low%1.3f" ++= " -> " ++= f"$probability%1.3f" ++= " -> " ++= f"$high%1.3f" +"]"
    str.toString
  }
}

class Node(val lambdaForPoisson:Double)  extends Serializable  {


  def createChildren() : List[Node] = {
    val numberOfChildren = utils.Statistics.nextRandomDescendantsPoisson(this.lambdaForPoisson)
     List.fill(numberOfChildren)(new Node(this.lambdaForPoisson))
  }
}
class GaltonWatsonSpark(val maxPopulation:Long= 100, val seedNode:Node = new Node(lambdaForPoisson = 1.0))
  extends Serializable
{


  var livingNodes:immutable.List[Node]= immutable.List[Node]()

  private var _time = 0L
  def time(): Long = _time

  private var _isSeedDominant = false
  def isSeedDominant():Boolean = _isSeedDominant

  def run() :GaltonWatsonSpark = {
    //logger.info(s"Galton-Watson experiment started with lambda=${this.seedNode.lambdaForPoisson}" )

    livingNodes = seedNode :: livingNodes
    while(this.livingNodes.nonEmpty && !this.isSeedDominant ) {
      var nextGenNodes = List[Node]()
      for(node <- livingNodes) {
        val children = node.createChildren()
        if (children.size + nextGenNodes.size < maxPopulation)
          nextGenNodes = nextGenNodes ::: children
        else
          this._isSeedDominant = true

      }
      this.livingNodes = nextGenNodes
      _time +=1
    }
    this
  }
}

object GaltonWatsonSpark  {

  def main(args : Array[String]): Unit = {

    time {
      val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("Replicator Experiment")
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val dims = for(
        lambda <- BigDecimal(1.0) to BigDecimal(1.6) by BigDecimal(0.1);
        n <- 1 to 5000
        ) yield (lambda,n)

      val dimensions = spark.sparkContext.parallelize(dims,10)


      println(s"Galton-Watson Simulation started with ${dimensions.count()} trials")



      val results = dimensions
        .map({case (lambda,n)
                  => new GaltonWatsonSpark(maxPopulation = 1000, seedNode = new Node(lambda.doubleValue)) })
        .map(gw => gw.run()).cache()


      // -- calculate and show expected extinction time by lambda
      val expExtinctionTime = results.groupBy(gw => gw.seedNode.lambdaForPoisson)
          .map({ case (lambda, gws )
          => (lambda,gws.filter(! _.isSeedDominant() )
            .map(gw => gw.time()).reduce(_+_).toDouble / gws.count(! _.isSeedDominant() ))})

      println("\nExpected Extinction time by lambda  ")
      expExtinctionTime.sortByKey().collect().foreach(
        t=> println(s"  -  E(t(*)|lambda =${t._1}) = "+ f"${t._2}%1.1f"))

      // -- calculate and show survival probabilities at various lambdas

      val confidence = 0.95
       val survivalProbabilitiesByLambda =
        results.groupBy(gw => gw.seedNode.lambdaForPoisson)
          .map({ case (lambda, gws ) => (lambda, gws.map(gw => if(gw.isSeedDominant()) 1.0 else 0.0))})
          .map({ case (lambda, zerosAndOnes )
          => (lambda, ProbabilityWithConfidence(probability=zerosAndOnes.reduce(_+_)/zerosAndOnes.size,
            confidence=confidence,
            Statistics.confidenceInterval(List() ++ zerosAndOnes, 1-confidence)._1,
            Statistics.confidenceInterval(List() ++ zerosAndOnes, 1-confidence)._2 ))})


      println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )

      survivalProbabilitiesByLambda.sortByKey().collect().foreach(
        { case (lambda, probabilityWithConfidence: ProbabilityWithConfidence )
        => println(s"  - P(survival|lambda=$lambda) = ${probabilityWithConfidence.toString}")

        })

      spark.stop()


    }


  }
}

