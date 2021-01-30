package org.montecarlo

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{avg, count, lit, stddev}
import org.apache.spark.sql.{RelationalGroupedDataset, Row}
import org.montecarlo.utils.Statistics

import scala.collection.mutable.ArrayBuffer

class MvRelationalGroupedDataset(val rgds:RelationalGroupedDataset) {

  /**
   * Takes  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   * @param rgds i.e. df.groupBy("whateverCol")
   * @param valuesCol The name of the column with numeric values, the base of calculation
   * @param requestedConfidences the requested confidence levels. Each value produces a new row
   * @return
   */
  def calculateConfidenceIntervals(valuesCol:String,
                                   requestedConfidences:Seq[Double]=Seq(0.99)): MvDataFrame = {

    val aggregatedDF = rgds.agg(
      avg(valuesCol).as("mean"),
      stddev(valuesCol).as("stdDev"),
      count(valuesCol).as("count")
    ).withColumn("low", lit(0.0))
      .withColumn("high", lit(0.0))
      .withColumn("conf", lit(0.0))

    MvDataFrame(aggregatedDF.flatMap{row => {
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
    }}(RowEncoder(aggregatedDF.schema)))
  }
  /**
   * Takes  one of its columns with numeric values (valuesCol), as samples.
   * Then the mean is calculated with lower and upper bounds with the requested confidence level.
   * @param valuesCol The name of the column with numeric values, the base of calculation
   * @param requestedConfidence the requested confidence level.
   * @return
   */
  def calculateConfidenceInterval(valuesCol:String, confidence: Double =0.95):  MvDataFrame =
    this.calculateConfidenceIntervals(valuesCol,Seq(confidence))

  def orderBy(input: Input): Unit = {

  }
}
