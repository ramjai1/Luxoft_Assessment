package com.cs.dsl

import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.math.min

/**
  * Created by rjaiswa8 on 8/10/2018.
  */
object SensorStatistics {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "C:\\Users\\rjaiswa8\\Downloads\\Docs\\SparkScala")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SensorStatistics").master("local").getOrCreate()
    import spark.implicits._
    val schema = Encoders.product[SensorStatic].schema
    val dirPath = if (args.length == 0) "src/main/resources/data" else args(0).toString
    val groupedDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(schema).load(dirPath).as[SensorStatic]

    groupedDF.persist(StorageLevel.MEMORY_ONLY)

    println("Num of processed files:" + new GetFileCount().getFilesCount(dirPath))

    val distributedwatch = new Distributedwatch(spark.sparkContext)
    for (k <- groupedDF.rdd.collect()) {
      k.humidity match {
        case "NaN" => distributedwatch.incrementFaileMeasurment
        case _ => distributedwatch.incrementProcessedMeasurment
      }
    }

    println("Num of processed measurements:" + distributedwatch.getProcessedMeasurment())
    println("Num of failed measurements:" + distributedwatch.getFailedMeasurment())

    println("Sensors with highest avg humidity:")
    println("sensor-id,min,avg,max")
    groupedDF.rdd.cache()
    val sensorRdd = groupedDF.filter($"humidity" =!= "NaN").rdd.map(row => (row.sensor_id, row.humidity.toInt)) //.collect().foreach(x => println(x._2))
    //val com = pairRdd.map(x => (x._1, x._2))

    val tup = sensorRdd.aggregateByKey((Int.MaxValue, 0, 0, 0))(
      {
        case ((min1, sum1, max1, count1), (v1)) =>
          (v1 min min1, sum1 + v1, v1 max max1, count1 + 1)
      }, {
        case ((min1, sum1, max1, count),
        (otherMin1, otherSum1, otherMax1, otherCount)) =>
          (min1 min otherMin1, sum1 + otherSum1, max1 max otherMax1, count + otherCount)
      }
    )
      .map {
        case (k, (min1, sum1, max1, count1)) => (k, (min1, sum1 / count1, max1))
      }

    tup.foreach(x => println(x._1 + "," + x._2))

    val sensorListWithNan = groupedDF.rdd.map(row => (row.sensor_id, row.humidity)).groupByKey
    for ((k, v) <- sensorListWithNan.collect()) {
      if (v.toList.length == 1 && v.toList(0) == "NaN")
        println(k.toString + ",(NaN," + "NaN," + "Nan)")
    }
  }
}