package org.montecarlo

import ch.qos.logback.classic.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import org.montecarlo.utils.Statistics
import org.slf4j.LoggerFactory

class WeightedErrorUDAF extends UserDefinedAggregateFunction {

  val log: Logger = LoggerFactory.getLogger(getClass.getName).asInstanceOf[Logger]
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: StructField("weight", DoubleType) :: StructField("confidence", DoubleType) ::  Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("weight", DoubleType) ::
      StructField("weightedSumValue", DoubleType) ::
      StructField("weightedSumValue2", DoubleType) ::
      StructField("confidence", DoubleType) ::
      Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
    buffer(2) = 0.0
    buffer(3) = 0.9
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val x = input.getDouble(0) * input.getDouble(1)
    buffer(0) = buffer.getDouble(0) + input.getDouble(1)
    buffer(1) = buffer.getDouble(1) + x
    buffer(2) = buffer.getDouble(2) + input.getDouble(0)* input.getDouble(0)*input.getDouble(1)
    buffer(3) = input.getDouble(2)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
    buffer1(2) = buffer1.getDouble(2) + buffer2.getDouble(2)
    buffer1(3) = buffer2(3)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val weight = buffer.getDouble(0)
    val mean = buffer.getDouble(1) /weight
    val stdDev = Math.sqrt(buffer.getDouble(2) /weight - mean*mean)
    if (weight != 0) {
      val conf = buffer.getDouble(3)
      val (low, high) = Statistics.confidenceInterval(weight.toLong, mean, stdDev, conf)
      mean-low
    } else {
      log.warn("Confidence calculation is not possible with less then 2 sample size. " +
        "Returning <Double.MaxValue> as error.")
      Double.MaxValue
    }

  }
}
