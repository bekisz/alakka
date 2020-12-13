package org.alakka.galtonwatson

import org.alakka.utils.{ProbabilityWithConfidence, Statistics}
import org.alakka.utils.Time.time
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



case class Dimension(lambda:Double, trialNo:Int)
case class ResultsByLambda(lambda:Double, probabilityWithConfidence:ProbabilityWithConfidence, noOfTrials:Long) {


}
object Experiment  {



  def main(args : Array[String]): Unit = {

    time {
      val conf = new SparkConf().setAppName("Galton-Watson Experiment")

      val spark = SparkSession.builder.config(conf).getOrCreate()

      val trialsMultiplier:Int = if (args.size > 0)  { args(0).toInt } else 1000

      val lambdas = for(lambda <- BigDecimal(1.0) to BigDecimal(1.6) by BigDecimal(0.1)) yield(lambda.toDouble)
      val trialsMultipliers = for( n <- 1 to trialsMultiplier) yield (n)

      import spark.implicits._
      val lambdasDF =  spark.sparkContext.parallelize(lambdas).toDF.withColumnRenamed("value","lambda")
      val trialsNosDF = spark.sparkContext.parallelize(trialsMultipliers).toDF.withColumnRenamed("value","trialNo")
      val dimensionDS = lambdasDF.crossJoin(trialsNosDF).as[Dimension]

      println(s"Galton-Watson Simulation started with ${dimensionDS.count()} trials")

      val results = dimensionDS
        .map(t =>  new Trial(maxPopulation = 1000, seedNode = new Node(t.lambda)).run().toCase() ).cache()


      // -- calculate and show expected extinction time by lambda


      val expExtinctionTime = results.where($"isSeedDominant" === false)
        .groupBy("lambda").agg(
          format_number(avg($"time"),1).as("extinctionTime"))
        .orderBy("lambda").show()

      val survivalProb = results
        .groupBy("lambda").agg(
        avg($"isSeedDominant".cast("Integer")).as("survivalProbability"),
        stddev($"isSeedDominant".cast("Integer")).as("stdDevProbability"),
        count($"isSeedDominant").as("noOfTrials"))



      val confidence = 0.95

      println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
      val survivalProbConf = survivalProb.map {
              row =>
                val lambda = row.getAs[Double](0)
                val survivalProbability =row.getAs[Double](1)
                val stdDevProbability =row.getAs[Double](2)
                val noOfTrials =row.getAs[Long](3)
                val (confidenceIntervalLow, confidenceIntervalHigh)
                  = Statistics.confidenceInterval (noOfTrials, survivalProbability, stdDevProbability, confidence)

                ResultsByLambda(lambda, ProbabilityWithConfidence(survivalProbability, confidence,confidenceIntervalLow, confidenceIntervalHigh),
                  noOfTrials)
      }.sort($"lambda".asc).collect().foreach( result => println(s"  - P(survival|lambda=${result.lambda}) = ${result.probabilityWithConfidence.toString}"))



 /*
      val confidence = 0.95
      val survivalProbabilitiesByLambda = results.collect().groupBy(trial => trial.lambda)
        .map({ case (lambda, gws ) => (lambda, gws.map(gw => if(gw.isSeedDominant) 1.0 else 0.0))})
        .map({ case (lambda, zerosAndOnes )
        => (lambda, ProbabilityWithConfidence(probability=zerosAndOnes.reduce(_+_)/zerosAndOnes.size,
          confidence=confidence,
          Statistics.confidenceInterval(List() ++ zerosAndOnes, 1-confidence)._1,
          Statistics.confidenceInterval(List() ++ zerosAndOnes, 1-confidence)._2 ))})

      println(s"\nSurvival Probabilities within ${confidence*100}% confidence interval by lambdas" )
      //SortedMap[Double,ProbabilityWithConfidence]() ++survivalProbabilitiesByLambda
      (SortedMap[Double,ProbabilityWithConfidence]() ++ survivalProbabilitiesByLambda).foreach(
        { case (lambda, probabilityWithConfidence: ProbabilityWithConfidence )
        => println(s"  - P(survival|lambda=$lambda) = ${probabilityWithConfidence.toString}")

        })

   */
      spark.stop()


    }


  }
}
