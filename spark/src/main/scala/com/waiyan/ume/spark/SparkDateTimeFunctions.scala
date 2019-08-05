package com.waiyan.ume.spark

import java.time.ZoneId
import java.util.Calendar

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object SparkDateTimeFunctions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", true)
      .getOrCreate()

    // Reading from Seq or CSV with a schema
    val reader = new SparkReader(spark)
    val fruitsDF = reader.readFruitsCsv()

    val read = false

    if(!read) {
      println(s"${spark.conf.get("spark.sql.session.timeZone")} before setting spark.sql.session.timeZone to UTC")
      writeDf(fruitsDF, "/tmp/timezones/1")

      spark.conf.set("spark.sql.session.timeZone", "UTC")
      println(s"${spark.conf.get("spark.sql.session.timeZone")} after setting spark.sql.session.timeZone to UTC")
      writeDf(fruitsDF, "/tmp/timezones/2")
    } else {
      val df1 = spark.read.parquet("/tmp/timezones/1").withColumn("path", lit("/tmp/timezones/1"))
      val df2 = spark.read.parquet("/tmp/timezones/2").withColumn("path", lit("/tmp/timezones/2"))
      df1.union(df2).show(false)
    }
  }

  def writeDf(fruitsDF: DataFrame, path: String) = {
    val localNow = java.time.LocalDateTime.now()
    val utcNow = java.time.LocalDateTime.now(ZoneId.of("UTC"))
    val sqlNow = new java.sql.Time(Calendar.getInstance().getTime.getTime)
    val currentTimestamp = org.apache.spark.sql.functions.current_timestamp()

    val df = fruitsDF
      .withColumn("current_timestamp", currentTimestamp)
      .withColumn("localNow", lit(localNow.toString))
      .withColumn("utcNow", lit(utcNow.toString))
      .withColumn("sqlNow", lit(sqlNow.toString))
      .select("current_timestamp", "localNow", "utcNow", "sqlNow")
      .distinct
    df.show(false)
    df.coalesce(1).write.mode("overwrite").parquet(path)
  }

}
