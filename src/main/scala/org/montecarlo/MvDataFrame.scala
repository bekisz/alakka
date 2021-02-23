package org.montecarlo

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{avg, count, lit, stddev}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.montecarlo.utils.{ConfidenceInterval, Statistics}

import scala.collection.mutable.ArrayBuffer

class MvDataFrame(val df: DataFrame) {
  protected val spark: SparkSession = this.df.sparkSession
  private[this] var name: String = "aDataFrame"

  def setName(name: String): MvDataFrame = {
    this.name = name
    this
  }

  def getName(): String = this.name

  def groupBy(input: Input): MvRelationalGroupedDataset = {
    val dimensions = input.fetchDimensions()
    if (this.isOutputColumnsMatchInput(input) && dimensions.nonEmpty) {
      if (dimensions.length == 1) new MvRelationalGroupedDataset(this.df.groupBy(dimensions.head))
      new MvRelationalGroupedDataset(this.df.groupBy(dimensions.head, dimensions.tail: _*))
    } else throw new IllegalArgumentException("Input dimensions (" + input.fetchDimensions().mkString(", ")
      + ") mismatches output columns (" + this.df.columns.mkString(", ") + ")")

  }

  /**
   * Takes the single column Dataframe with numeric values as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param confidenceLevel the request confidence level
   * @return The ConfidenceInterval with calculated average, lower bound, upper bound
   *         and the given confidence level
   */
  def calculateConfidenceInterval(confidenceLevel: Double = 0.95): ConfidenceInterval
  = calculateConfidenceIntervals(Seq(confidenceLevel)).first()

  /**
   * Takes a Dataframe, or more narrowly  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   *
   * @param confidenceLevels the requested confidence levels. Each value produces a new row
   * @return Dateset of ConfidenceIntervals that include
   *         the calculated average, lower bound, upper bound and the given confidence level
   */
  def calculateConfidenceIntervals(confidenceLevels: Seq[Double] = Seq(0.95)): Dataset[ConfidenceInterval] = {
    import df.sparkSession.implicits._
    if (df.columns.length != 1) throw new IllegalArgumentException("Single column Dataframe expected."
      + s"This dataframe has ${df.columns.length} columns. Use select to narrow.")

    val colName = df.columns.head

    df.agg(
      avg(colName),
      stddev(colName),
      count(colName)
    ).flatMap {
      row =>
        val (mean, stdDev, count) = (row.getAs[Double](0), row.getAs[Double](1), row.getAs[Long](2))
        confidenceLevels.map { conf =>
          val (low, high) = Statistics.confidenceInterval(count, mean, stdDev, conf)
          ConfidenceInterval(mean, low, high, conf, count)
        }
    }
  }

  /**
   * Alias for [[MvDataFrame.sort]].
   * Returns a new Dataset sorted by the experiment [[Input]] dimensions in ascending order.
   *
   * @param input The [[Input]] that was given to the experiment
   * @return A new, ordered MvDataFrame
   */
  def orderBy(input: Input): MvDataFrame = this.sort(input)

  /**
   * Returns a new Dataset sorted by the experiment [[Input]] dimensions in ascending order.
   *
   * @param [[Input]] to the experiment
   * @return A new, ordered MvDataFrame
   */

  def sort(input: Input): MvDataFrame = {
    val dimensions = input.fetchDimensions()
    if (this.isOutputColumnsMatchInput(input) && dimensions.nonEmpty) {
      if (dimensions.length == 1) MvDataFrame(this.df.sort(dimensions.head))
      MvDataFrame(this.df.sort(dimensions.head, dimensions.tail: _*))
    } else throw new IllegalArgumentException("Input dimensions (" + input.fetchDimensions().mkString(", ")
      + ") mismatches output columns (" + this.df.columns.mkString(", ") + ")")

  }

  def isOutputColumnsMatchInput(input: Input): Boolean
  = input.fetchDimensions().map {
    this.df.columns.contains(_)
  }.reduce(_ & _)

  /**
   * Trials are not producing output rows after they have finished. This can be a problem when  we
   * want to compare and analyze the trials in any given turn. To avoid this, this method can prolong the
   * trials till [[prolongTillTurn]] filling the last output with the trailing turns.
   *
   * Needed column names : "turn", "isFinished"
   *
   * @param prolongTillTurn
   * @return an OutputDF with prolonged turns and with an extra column isProlonged
   */
  def retroActivelyProlongTrials(prolongTillTurn: Int): MvDataFrame = {
    import spark.implicits._
    val dfNew = this.df.withColumn("isProlonged", lit(false) )
    MvDataFrame(
      dfNew.filter($"turn" <= prolongTillTurn).flatMap({ row =>
        var rows = row :: List[Row]()
        val rowTurn = row.getAs[Long]("turn")
        val turnIndex = row.fieldIndex("turn")
        val isProlongedIndex = row.fieldIndex("isProlonged")
        if (row.getAs[Boolean]("isFinished")) {
          rows = rows :::
            (rowTurn + 1 to prolongTillTurn).map(newTurn => {
              val buffer = ArrayBuffer[Any](row.toSeq: _*)
              buffer(turnIndex) = newTurn
              buffer(isProlongedIndex) = true
              Row.fromSeq(buffer)
            }).toList
        }
        rows
      })(RowEncoder(dfNew.schema)))
  }

  /**
   * The number of trials in the dataframe.
   *
   * @return
   */
  def countTrials(): Long = this.df.select("trialUniqueId").distinct().count()

  /**
   * The number of turns in the dataframe.
   *
   * @return
   */
  def countTurns(): Long = {
    import df.sparkSession.implicits._
    this.df.filter($"turn" > 0).select("trialUniqueId", "turn").distinct().count()
  }
}

object MvDataFrame {
  def apply(df: DataFrame) = new MvDataFrame(df)
}