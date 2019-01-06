package com.waiyan.ume.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, split }

object SparkPartitioning {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", true)
      .getOrCreate()

    // Reading from Seq or CSV with a schema
    val reader = new SparkReader(spark)

    val ts1RawDf = reader.readTimeSeriesCsv(1)
    val ts2RawDf = reader.readTimeSeriesCsv(2)

    // Split a column into multiple columns
    val ts1Df = ts1RawDf
      .withColumn("temp", split(col("date"), "-"))
      .select(
        col("*"),
        col("temp").getItem(0).as("year"),
        col("temp").getItem(1).as("month"),
        col("temp").getItem(2).as("day")
      )
      .drop("temp")
    ts1Df.show

    val ts2Df = ts2RawDf
      .withColumn("temp", split(col("date"), "-"))
      .select(
        col("*"),
        col("temp").getItem(0).as("year"),
        col("temp").getItem(1).as("month"),
        col("temp").getItem(2).as("day")
      )
      .drop("temp")
    ts2Df.show

    ts1Df.write.mode("append").partitionBy("year", "month", "day").csv("file:///tmp/output/timeseries")
    ts2Df.write.mode("append").partitionBy("year", "month", "day").csv("file:///tmp/output/timeseries")

    // Reading data for a specific day
    val ts2PartitionedDf = spark.read.csv("file:///tmp/output/timeseries/year=2019/month=01/day=02")
    ts2PartitionedDf.show
  }

}
