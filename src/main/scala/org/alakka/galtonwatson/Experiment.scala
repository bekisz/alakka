package org.alakka.galtonwatson

import org.alakka.utils.ProbabilityWithConfidence
import org.alakka.utils.Time.time
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalameter.utils.Statistics



object Experiment  {

  def main(args : Array[String]): Unit = {

    time {
      val conf = new SparkConf().setAppName("Replicator Experiment")
      val spark = SparkSession.builder.config(conf).getOrCreate()

      val trialsMultiplier:Int = if (args.size > 0)  { args(0).toInt } else 1000
      val dimensions = for(
        lambda <- BigDecimal(1.0) to BigDecimal(1.6) by BigDecimal(0.1);
        n <- 1 to trialsMultiplier
      ) yield (lambda,n)

      val dimensionsRDD = spark.sparkContext.parallelize(dimensions,20)

      println(s"Galton-Watson Simulation started with ${dimensionsRDD.count()} trials")

      val results = dimensionsRDD
        .map({case (lambda,n)
        => new Trial(maxPopulation = 1000, seedNode = new Node(lambda.doubleValue)) })
        .map(gw => gw.run()).cache()


      // -- calculate and show expected extinction time by lambda
      val expExtinctionTime: RDD[(Double, Double)] = results.groupBy(gw => gw.seedNode.lambdaForPoisson)
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
