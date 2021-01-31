package org.montecarlo

import org.apache.spark.sql.functions.{avg, count, stddev, sum}
import org.apache.spark.sql.{DataFrame, Dataset,  SparkSession}
import org.montecarlo.utils.{ConfidenceInterval, Statistics}

class MvDataFrame(val df : DataFrame)  {
  protected val spark:SparkSession = this.df.sparkSession


  /**
   * @return # of turns summed in the experiment
   */
  def turns() : Long = {
    import spark.implicits._
    this.df.filter($"isFinished" ).select(sum("turn")).first().getAs[Long](0)
  }
  /**
   * @return # of trial executed
   */
  def trials() : Long = {
    import spark.implicits._
    this.df.filter($"isFinished" ).count()
  }
  def isOutputColumnsMatchInput(input:Input) : Boolean
    = input.fetchDimensions().map{this.df.columns.contains(_)}.reduce(_&_)

  def groupBy(input:Input) : MvRelationalGroupedDataset = {
    val dimensions = input.fetchDimensions()
    if (this.isOutputColumnsMatchInput(input) && dimensions.nonEmpty) {
      if (dimensions.length == 1) new MvRelationalGroupedDataset(this.df.groupBy(dimensions.head))
      new MvRelationalGroupedDataset(this.df.groupBy(dimensions.head, dimensions.tail: _*))
    } else throw new IllegalArgumentException("Input dimensions (" + input.fetchDimensions().mkString(", ")
      + ") mismatches output columns (" + this.df.columns.mkString(", ") + ")")

  }

  /**
   * Takes a Dataframe, or more narrowly  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param confidenceLevels the requested confidence levels. Each value produces a new row
   * @return Dateset of ConfidenceIntervals that include
   *         the calculated average, lower bound, upper bound and the given confidence level
   */
  def calculateConfidenceIntervals(confidenceLevels:Seq[Double]=Seq(0.95)): Dataset[ConfidenceInterval] = {
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
          ConfidenceInterval(mean, low, high, conf,count)
        }
    }
  }


  /**
   * Takes the single column Dataframe with numeric values as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param confidenceLevel the request confidence level
   * @return The ConfidenceInterval with calculated average, lower bound, upper bound
   *         and the given confidence level
   */
  def calculateConfidenceInterval(confidenceLevel:Double=0.95): ConfidenceInterval
  = calculateConfidenceIntervals(Seq(confidenceLevel)).first()

/**
 * Returns a new Dataset sorted by the experiment [[Input]] dimensions in ascending order.
 * @param [[Input]] to the experiment
 * @return A new, ordered MvDataFrame
 */

  def sort(input:Input): MvDataFrame = {
    val dimensions = input.fetchDimensions()
    if (this.isOutputColumnsMatchInput(input) && dimensions.nonEmpty) {
      if (dimensions.length == 1) MvDataFrame(this.df.sort(dimensions.head))
      MvDataFrame(this.df.sort(dimensions.head, dimensions.tail: _*))
    } else throw new IllegalArgumentException("Input dimensions (" + input.fetchDimensions().mkString(", ")
      + ") mismatches output columns (" + this.df.columns.mkString(", ") + ")")

  }

  /**
   * Alias for [[MvDataFrame.sort()]].
   * Returns a new Dataset sorted by the experiment [[Input]] dimensions in ascending order.
   *
   * @param input The [[Input]] that was given to the experiment
   * @return A new, ordered MvDataFrame
   */
  def orderBy(input: Input) :MvDataFrame = this.sort(input)
}

object MvDataFrame {
  def apply(df : DataFrame) = new MvDataFrame(df)
}