package org.alakka.galtonwatson

import java.util.UUID

import org.alakka.utils.Time.time
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.montecarlo.Parameter

/**
 * Implementation of the Galton-Watson experiment by various lambda values and Monte-Carlo trials
 *
 * @param name The name of the experiment that is got propagated to SparkSession, in case that one is newly created
 * @param monteCarloMultiplicity The number of trials executed for each lambda in the lambdaRange Seq
 * @param trialInputParams The range of variables for the trials that are executed. Each value will executed by monteCarloMultiplicity times
 *
 */
class Experiment(val name:String,
                 val trialInputParams: GwTrialInputParameters,
                 val monteCarloMultiplicity:Int = 1000
                ) {


  val spark: SparkSession = SparkSession.builder.appName(name).getOrCreate()
  val allTrialInputDS:RDD[GwTrialInputParameters] = this.initAllTrialInputsDS().cache()
  //val flattenedAllTrialInputDF:Dataset[Row] = this.initFlattenedAllTrialInputDF().cache()

  private[this] def initAllTrialInputsDS(): RDD[GwTrialInputParameters] = {

    import spark.implicits._
    //val inputParametersDF = this.trialInputParams.createPermutations().toDF()
    val inputParametersDF = spark.sparkContext.parallelize(this.trialInputParams.createPermutations())

    val multiplicityDS = (1 to this.monteCarloMultiplicity).toDS()
    //inputParametersDF.cartesian(mu)
    multiplicityDS.rdd.cartesian(inputParametersDF).map(
      t =>
      t._2.copy(trialUniqueId = UUID.randomUUID().toString))


    /*
    multiplicityDS.rdd.cartesian(inputParametersDF).toDF()
      .drop("value")
      .withColumn("trialUniqueId", expr("uuid()"))
      .as[GwTrialInputParameters]
  */
  }
  /*
  private[this] def initFlattenedAllTrialInputDF() : Dataset[Row] = {


    var newSchema = new StructType()

    for (parameterField <- this.allTrialInputDS.schema.toList) {

      var typ = parameterField.dataType
      parameterField.dataType match {
        case structType: StructType => typ = structType.head.dataType.asInstanceOf[ArrayType].elementType
        case _ =>
      }
      newSchema = newSchema.add(parameterField.name, typ)
    }


    val newData = this.allTrialInputDS.toDF()
      .collect()
      .map(row =>
      { val anySeq = {
        for( parameters <- row.toSeq) yield {
          var yielded = parameters
          parameters match {
            case genericRow:GenericRowWithSchema =>
              genericRow.get(0) match {
                case genericRow0:mutable.IndexedSeq[Any]
                    =>  yielded = genericRow0.head
                case  _ =>
              }
            case  _ =>
          }
          yielded
        }
      }
        Row.fromSeq(anySeq)
      }).toIndexedSeq
    spark.createDataFrame(spark.sparkContext.parallelize(newData),newSchema)

  }*/
  def run(): Dataset[TrialOutput] = {

   // println(s"Running ${this.spark.sparkContext.appName} with ${this.allTrialInputDS.count()} trials")
    println("Spark version : " + this.spark.sparkContext.version)

    import spark.implicits._

    val trialOutputDS:Dataset[TrialOutput] = this.allTrialInputDS.flatMap(trialInput => {
      /*
      val trial = new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation,
        seedNode = new Node(trialInput.lambda ))
      */

      val trial = trialInput.buildTrial(trialInput)
      var outputList = List[TrialOutput]()
      if (trialInput.dataCollectionCondition(trial)) outputList = TrialOutput(trial.asInstanceOf[GwTrial]) :: outputList

      while (!trial.isFinished) {
        trial.tick()
        if (trialInput.dataCollectionCondition(trial)) outputList = TrialOutput(trial.asInstanceOf[GwTrial]) :: outputList
      }
      outputList

    }).toDS()



    /*
    val trialOutputDS:Dataset[TrialOutput] = if (this.enableInTrialOutputData) {
      this.allTrialInputDS.flatMap(trialInput => {

          val trial = new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation,
            seedNode = new Node(trialInput.lambda ))
          var outputList = List[TrialOutput](TrialOutput(trial))
          while (!trial.isFinished) {
            trial.tick()
            outputList = TrialOutput(trial) :: outputList
          }
          outputList

        })
    } else {
      this.allTrialInputDS.map(trialInput =>
        TrialOutput( new GwTrial(trialInput.trialUniqueId, trialInput.maxPopulation,
          seedNode = new Node(trialInput.lambda)).run()) )
    } */
    trialOutputDS
  }

}

object Experiment {

  def main(args : Array[String]): Unit = {

    time {
      val monteCarloMultiplicity = if (args.length > 0)  args(0).toInt  else 1000
      import Parameter.implicitConversions._
      val inputParameters = GwTrialInputParameters(
        lambda = Vector(1.2, 2.0),
        maxPopulation = 1001,
        dataCollectionCondition = t=> t.time() % 2 == 0 || t.isFinished

      )
      val experiment = new Experiment("Galton-Watson Experiment", inputParameters,
        2)

      //println("--experiment.allTrialInputDS")

      //experiment.allTrialInputDS.show(100)
      //println("--experiment.flattenedAllTrialInputDF")

      //experiment.flattenedAllTrialInputDF.show(100)

      val trialOutputDS = experiment.run().cache()

      println("TrialOutputDS")
      trialOutputDS.show(100)
      //println("TrialInputOutputDS")
      //val inputOutputDF = experiment.flattenedAllTrialInputDF.join(trialOutputDS, "trialUniqueId")
      //inputOutputDF.show(100)

      val analyzer = new TrialOutputAnalyzer(trialOutputDS)

      analyzer.survivalProbabilityByLambda(confidence = 0.99)
        .collect().foreach( aggregatedOutput => println(aggregatedOutput.toString()))
      analyzer.averagePopulationByLambdaAndTime(10).show(100)


      analyzer.expectedExtinctionTimesByLambda().show()
      val ticks = analyzer.ticks()
      val trials = analyzer.trials()
      println(s"\n$ticks ticks (smallest unit of time) processed in $trials trials, averaging "
        + f"${ticks.toDouble / trials}%1.1f ticks/trial\n")

      experiment.spark.stop()


    }
  }
}
