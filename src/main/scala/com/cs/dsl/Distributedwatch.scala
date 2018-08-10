package com.cs.dsl

import org.apache.spark.{Accumulator, SparkContext}

class Distributedwatch(sc: SparkContext) {

  var processedMeasurement: Accumulator[Long] = sc.accumulator(0L, "Processed Measurement")
  var failedMeasurement: Accumulator[Long] = sc.accumulator(0L, "Failed Measurement")


  def incrementProcessedMeasurment(): Unit = {
    processedMeasurement+=1
  }

  def incrementFaileMeasurment(): Unit = {
    failedMeasurement+=1
  }

  def getProcessedMeasurment():Long = {
    processedMeasurement.value
  }

  def getFailedMeasurment():Long = {
    failedMeasurement.value
  }
}