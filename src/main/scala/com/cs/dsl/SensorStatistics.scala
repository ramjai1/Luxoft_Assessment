package com.cs.dsl

import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

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
    val groupedDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(schema).load("src/main/resources/data").as[SensorStatic]

    groupedDF.persist(StorageLevel.MEMORY_ONLY)
    println("Num of processed files:" + new GetFileCount().getFilesCount("src/main/resources/data"))

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
    val pairRdd = groupedDF.map(row => (row.sensor_id, row.humidity)).rdd.groupByKey()

    var sensorList = new ListBuffer[(String, Long, Long, Long)]()
    var sensorListWithNan = new ListBuffer[(String, String, String, String)]()
    for ((k, v) <- pairRdd.collect())
      if (v.toList.length == 1 && v.toList(0) == "NaN")
        sensorListWithNan += ((k.toString, "NaN", "NaN", "Nan"))
      else {
        val mapped = v.toList.filter(_ != "NaN").map(_.toLong)
        sensorList += ((k.toString, mapped.min, (mapped.sum / mapped.length), mapped.max))
        //println(s"$k,"+mapped.min + ","+(mapped.sum/mapped.length) + ","+mapped.max)
      }
    //sensorList.foreach(println)
    sensorList.sortBy(_._3).reverse.foreach(println)
    sensorListWithNan.foreach(println)
  }
}

