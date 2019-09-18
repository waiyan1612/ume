package com.waiyan.ume.spark

import java.time.ZoneId
import java.util.Calendar

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object SparkDateTimeFunctions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val tsSchema = StructType(Array(
      StructField("id", StringType, false),
      StructField("original_timestamp", StringType, false)
    ))

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("jkt", "2020-01-01T00:07:06.000+07:00"),
      Row("sgt", "2020-01-01T00:07:06.000+08:00"),
      Row("utc", "2020-01-01T00:07:06.000+00:00"),
      Row("no timezone", "2020-01-01T00:07:06.000")
    )),tsSchema)

    val localNow = java.time.LocalDateTime.now()
    val utcNow = java.time.LocalDateTime.now(ZoneId.of("UTC"))
    val sqlNow = new java.sql.Timestamp(Calendar.getInstance().getTime.getTime)
    val currentTimestamp = org.apache.spark.sql.functions.current_timestamp()

    // Reading from Seq or CSV with a schema
    val reader = new SparkReader(spark)
    val fruitsDF = reader.readFruitsCsv()

    fruitsDF
      .withColumn("current_timestamp", currentTimestamp)
      .withColumn("localNow", lit(localNow.toString))
      .withColumn("utcNow", lit(utcNow.toString))
      .withColumn("sqlNow", lit(sqlNow.toString))
      .select("current_timestamp", "localNow", "utcNow", "sqlNow")
      .distinct
      .show(false)


    println(s"${spark.conf.get("spark.sql.session.timeZone")} before setting spark.sql.session.timeZone to UTC")
    df1.withColumn("converted_timestamp", $"original_timestamp".cast("timestamp")).show(false)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df1.withColumn("converted_timestamp", $"original_timestamp".cast("timestamp")).show(false)
  }

}
