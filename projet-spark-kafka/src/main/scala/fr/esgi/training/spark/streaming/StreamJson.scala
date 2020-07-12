package fr.esgi.training.spark.streaming

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime

import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import org.apache.spark.sql.functions._

import scala.io.StdIn
import java.sql.Timestamp

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StreamJson {
  def main(args: Array[String]): Unit = {}

  val schema = (new StructType()
    .add("idDrone",IntegerType)
    .add("zone", (new StructType())
      .add("idRegion",IntegerType)
      .add("RegionState",StringType)
      .add("StreetCode1",StringType)
      .add("StreetCode2",StringType)
      .add("StreetCode3",StringType)
      .add("HouseNumber",StringType)
      .add("StreetName",StringType)
    )
    .add("coordonnée",(new StructType())
      .add("longitude",FloatType)
      .add("latitude",FloatType)
      )
    .add("currentDate",StringType)
    .add("currentTime",StringType)
    .add("violationType",IntegerType)
    .add("infraction",(new StructType())
      .add("code",StringType)
      .add("violationDescription",StringType)
      .add("idPhoto",StringType)
      )
    .add("vehicle",(new StructType())
      .add("plaque",StringType)
      .add("plateType",StringType)
      .add("VehicleBodyType",StringType)
      .add("VehicleMake",StringType)
      .add("VehicleColor",StringType))
  )



  val spark = SparkUtils.spark()

  import spark.implicits._
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.99.100:9092")
    .option("subscribe", "json")
    .load()
    .selectExpr("CAST(value AS STRING)")

  val aggregateDF = df
    .select(from_json(col("value"), schema).as("data"))
    .select(
            col("data.idDrone"),
            col("data.zone.*" ),
            col("data.coordonnée.*"),
            col("data.currentDate").cast(DateType),
            col("data.currentTime"),
            col("data.violationType"),
            col("data.infraction.*"),
            col("data.vehicle.*")
    )

  import java.util.concurrent.TimeUnit
  aggregateDF.writeStream
    .option("format", "append")
    .format("csv")
    .option("header", true)
    .option("checkpointLocation", "tmpJson_2/sparkcheckpoints")
    .option("path", "tmpJson/Historiquemsg/")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(1,TimeUnit.MINUTES))
    .start()
    .awaitTermination()



  /*
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

  var aggregateDFCount = aggregateDF.withWatermark("timestamp", "10 minutes")

    .groupBy(window(col("timestamp"), "1 minute", "10 seconds"),col("IdRegion"))
    .agg(count("IdRegion") as "countIdRegion")
    .select(col("IdRegion"),col("countIdRegion"))
*/


  //.selectExpr("CAST(value AS STRING)")
  /*
  var df = spark.readStream
    .format("socket")
    .option("host", "192.168.99.100")
    .option("port", 9092)
    .load()

  val aggregateDF = df
  val aggregateDF1 = aggregateDF.withColumn("value_splited",  split(col("value"), ";"))

  aggregateDF1.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination(3*60*100)

 */
}
