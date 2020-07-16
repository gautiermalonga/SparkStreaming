package fr.esgi.training.spark.streaming



import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import org.apache.spark.sql.functions._

import scala.io.StdIn
import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.spark_project.dmg.pmml.False
object streamAggregate {

  def main(args: Array[String]): Unit = {}

  val spark = SparkUtils.spark()

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.99.100:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)","key", "timestamp")

  val aggregateDF = df
    .withColumn("timestamp", col("timestamp"))
    .withColumn("value_s", split(col("value"), ";")).select(
    col("timestamp"),
    col("value_s").getItem(0).as("IdRegion"),
    col("value_s").getItem(1).as("IdDrone").cast(IntegerType),
    col("value_s").getItem(2).as("CurrentDate").cast(DateType),
    col("value_s").getItem(3).as("CurrentTime").cast(StringType),
    col("value_s").getItem(4).as("x").cast(FloatType),

    col("value_s").getItem(5).as("y").cast(FloatType),
    col("value_s").getItem(6).as("RegionState").cast(StringType),
    col("value_s").getItem(7).as("StreetCode1").cast(IntegerType),
    col("value_s").getItem(8).as("StreetCode2").cast(IntegerType),

    col("value_s").getItem(9).as("StreetCode3").cast(IntegerType),
    col("value_s").getItem(10).as("HouseNumber").cast(StringType),
    col("value_s").getItem(11).as("StreetName").cast(StringType),
    col("value_s").getItem(12).as("Violation").cast(IntegerType),

    col("value_s").getItem(13).as("IdPhoto").cast(StringType),
    col("value_s").getItem(14).as("Plaque").cast(StringType),
    col("value_s").getItem(15).as("PlateType").cast(StringType),
    col("value_s").getItem(16).as("VehicleBodyType").cast(StringType),

    col("value_s").getItem(17).as("VehicleMake").cast(StringType),
    col("value_s").getItem(18).as("VehicleColor").cast(StringType),
    col("value_s").getItem(19).as("ViolationCode").cast(IntegerType),
    col("value_s").getItem(20).as("ViolationDescription").cast(StringType)

  )


  var Iddrone = aggregateDF
    .where(col("Violation") === 0)
    .groupBy(window(col("timestamp"), "30 seconds" ),col("IdDrone"))
    .count()

  Iddrone
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  var code = aggregateDF
    .where(col("Violation") === 0)
    .groupBy(window(col("timestamp"), "30 seconds" ),col("ViolationCode"))
    .count()

  code
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  var VehiculeDF = aggregateDF
    .where(col("Violation") === 0)
    .groupBy(window(col("timestamp"), "30 seconds" ),col("VehicleBodyType"))
    .count()

    VehiculeDF
      .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()



  /*
  aggregateDF.withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("day", dayofmonth(col("timestamp")))
  .writeStream
  .outputMode("append")
    .format("parquet")
    .option("path","192.168.99.100:hdfs//namenode:namenode-8020/")
    .option("checkpointLocation", "192.168.99.100:hdfs//namenode:namenode-8020/")
    .partitionBy("year", "month", "day")
    .option("truncate", false)
    .start()

   */

}
