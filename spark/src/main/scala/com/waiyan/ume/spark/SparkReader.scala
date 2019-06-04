package com.waiyan.ume.spark

import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types._

class SparkReader(spark: SparkSession) {

  val customerSchema = StructType(
    Array(
      StructField("name", StringType, false),
      StructField("country", StringType, false)
    ))

  val fruitSchema = StructType(
    Array(
      StructField("customer", StringType, false),
      StructField("fruit", StringType, false),
      StructField("qty", IntegerType, true),
      StructField("cost", DoubleType, true)
    ))

  val timeSeriesSchema = StructType(
    Array(
      StructField("date", StringType, false),
      StructField("user", StringType, false),
      StructField("value", StringType, false)
    ))

  val urlSchema = StructType(
    Array(
      StructField("domain", StringType, false),
      StructField("url", StringType, false)
    ))

  def readCustomersCsv() = {
    val customersCsv = getClass.getResource("/data/customers.csv").getPath
    spark.read.format("csv").schema(customerSchema).load(customersCsv)
  }

  def readFruitsCsv() = {
    val fruitsCsv = getClass.getResource("/data/fruits.csv").getPath
    spark.read.format("csv").schema(fruitSchema).load(fruitsCsv)
  }

  def readTimeSeriesCsv(day: Int) = {
    val fruitsCsv = getClass.getResource(s"/data/timeseries$day.csv").getPath
    spark.read.format("csv").schema(timeSeriesSchema).load(fruitsCsv)
  }

  def readUrlCsv() = {
    val urlCsv = getClass.getResource(s"/data/urls.csv").getPath
    spark.read.format("csv").schema(urlSchema).load(urlCsv)
  }

  def readFromFruitSeq() = {
    val fruitSeq = Seq(
      Row("alice", "apple", 1, null),
      Row("alice", "orange", null, 3.5),
      Row("alice", "grapes", 2, 0.5),
      Row("bob", "strawberry", 1, 2.0),
      Row("bob", "mango", null, 3.5),
      Row("bob", "mango", 2, 0.5),
      Row("carol", "grapes", 1, 9.0),
      Row("carol", "mango", 3, 3.5),
      Row("carol", "apple", 2, 0.5)
    )
    spark.createDataFrame(spark.sparkContext.parallelize(fruitSeq), fruitSchema)
  }

}

object SparkReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", true)
      .getOrCreate()

    val reader = new SparkReader(spark)

    val fruitCsvDF = reader.readFruitsCsv()
    fruitCsvDF.show

    val fruitSeqDF = reader.readFromFruitSeq()
    fruitSeqDF.show
  }
}
