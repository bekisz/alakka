package org.montecarlo

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import org.montecarlo.utils.Statistics

class ErrorUDAF extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: StructField("confidence", DoubleType) ::  Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("sum", DoubleType) ::
      StructField("sum2", DoubleType) ::
      StructField("confidence", DoubleType) ::
      Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0

  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val x = input.getAs[Double](0)
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) + x
    buffer(2) = buffer.getAs[Double](2) + x*x
    buffer(3) = input.getAs[Double](1)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
    buffer1(3) = buffer2(3)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Double](1) / buffer.getAs[Long](0)
    val count = buffer.getAs[Long](0)
    val mean = buffer.getAs[Double](1) /count
    val stdDev = Math.sqrt(buffer.getAs[Double](2) /count - mean*mean)
    val conf = buffer.getAs[Double](3)
    val (low, high) = Statistics.confidenceInterval(count, mean, stdDev, conf)
    mean-low
  }
}
