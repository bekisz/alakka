package org.montecarlo

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{avg, count, lit, stddev, sum}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.montecarlo.utils.{ConfidenceInterval, Statistics}

import scala.collection.mutable.ArrayBuffer

abstract class Analyzer {
  protected val spark:SparkSession = this.getOutputDF.sparkSession
  def getOutputDF : DataFrame

  /**
   * @return # of turns summed in the experiment
   */
  def turns() : Long = {
    import spark.implicits._
    this.getOutputDF.filter($"isFinished" ).select(sum("turn")).first().getAs[Long](0)
  }
  /**
   * @return # of trial executed
   */
  def trials() : Long = {
    import spark.implicits._
    this.getOutputDF.filter($"isFinished" ).count()
  }
  def isOutputColumnsMatchInput(input:Input) : Boolean
    = input.fetchDimensions().map{this.getOutputDF.columns.contains(_)}.reduce(_&_)

  def groupBy(input:Input) : RelationalGroupedDataset = {
    val dimensions = input.fetchDimensions()
    if (this.isOutputColumnsMatchInput(input) && dimensions.nonEmpty) {
      if (dimensions.length == 1) this.getOutputDF.groupBy(dimensions.head)
      this.getOutputDF.groupBy(dimensions.head, dimensions.tail: _*)
    } else throw new IllegalArgumentException("Input dimensions (" + input.fetchDimensions().mkString(", ")
      + ") mismatches output columns (" + this.getOutputDF.columns.mkString(", ") + ")")
  }
}
object Analyzer {
  /**
   * Takes a [[RelationalGroupedDataset]], with  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   * @param groupedBy i.e. df.groupBy("whateverCol")
   * @param valuesCol The name of the column with numeric values, the base of calculation
   * @param requestedConfidences the requested confidence levels. Each value produces a new row
   * @return
   */
  def calculateConfidenceIntervalsFromGroups(groupedBy:RelationalGroupedDataset,
                                             valuesCol:String,
                                             requestedConfidences:Seq[Double]=Seq(0.99)): DataFrame = {

    val aggregatedDF = groupedBy.agg(
      avg(valuesCol).as("mean"),
      stddev(valuesCol).as("stdDev"),
      count(valuesCol).as("count")
    ).withColumn("low", lit(0.0))
      .withColumn("high", lit(0.0))
      .withColumn("conf", lit(0.0))

    aggregatedDF.flatMap{row => {
      val (mean, stdDev, count) = (row.getAs[Double]("mean"),
        row.getAs[Double]("stdDev"), row.getAs[Long]("count"))
      requestedConfidences.map { conf =>
        val (low, high) = Statistics.confidenceInterval(count, mean, stdDev, conf)
        val buffer = ArrayBuffer[Any](row.toSeq:_*)
        buffer(buffer.size-3) = low
        buffer(buffer.size-2) = high
        buffer(buffer.size-1) = conf
        Row.fromSeq(buffer)
      }
    }}(RowEncoder(aggregatedDF.schema))
  }

  /**
   * Takes a Dataframe, or more narrowly  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param df DataFrame
   * @param confidenceLevels the requested confidence levels. Each value produces a new row
   * @return Dateset of ConfidenceIntervals that include
   *         the calculated average, lower bound, upper bound and the given confidence level
   */
  def calculateConfidenceIntervals(df:DataFrame,
                                   confidenceLevels:Seq[Double]=Seq(0.99)): Dataset[ConfidenceInterval] = {
    import df.sparkSession.implicits._
    if (df.columns.length != 1) throw  new IllegalArgumentException("Single column Dataframe expected."
      + s"This dataframe has ${df.columns.length} columns. Use select to narrow.")

    val colName = df.columns.head

    df.agg(
      avg(colName),
      stddev(colName),
      count(colName)
    ).flatMap {
      row =>
        val (mean,stdDev, count ) = (row.getAs[Double](0), row.getAs[Double](1), row.getAs[Long](2))
        confidenceLevels.map { conf =>
          val (low, high) = Statistics.confidenceInterval(count, mean, stdDev, conf)
          ConfidenceInterval(mean, low, high, conf)
        }
    }
  }


  /**
   * Takes the single column Dataframe with numeric values as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param df DataFrame
   * @param confidenceLevel the request confidence level
   * @return The ConfidenceInterval with calculated average, lower bound, upper bound
   *         and the given confidence level
   */
  def calculateConfidenceInterval(df:DataFrame,
                                  confidenceLevel:Double=0.99): ConfidenceInterval
  = calculateConfidenceIntervals(df, Seq(confidenceLevel)).first()
}
