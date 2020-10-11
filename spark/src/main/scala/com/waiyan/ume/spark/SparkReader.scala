package com.waiyan.ume.spark

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

class SparkReader(spark: SparkSession) {

  def generateIntDf(): DataFrame = {
    val intSchema = StructType(Array(StructField("id", StringType, false), StructField("value", IntegerType, false)))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("a", 1),
      Row("b", 2)
    )), intSchema)
  }

  def generateDoubleDf(): DataFrame = {
    val doubleSchema = StructType(Array(StructField("id", StringType, false), StructField("value", DoubleType, false)))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("a", 1.0),
      Row("b", 2.0)
    )), doubleSchema)
  }

  def generateStringDf(): DataFrame = {
    val stringSchema = StructType(Array(StructField("id", StringType, false), StructField("value", StringType, false)))
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row("a", "1"),
      Row("b", "2")
    )), stringSchema)
  }

  def readCustomersCsv(): DataFrame = {
    val customersCsv = getClass.getResource("/data/customers.csv").getPath
    val customerSchema = StructType(
      Array(
        StructField("name", StringType, false),
        StructField("country", StringType, false)
      ))
    spark.read.schema(customerSchema).csv(customersCsv)
  }

  def readFruitsCsv(): DataFrame = {
    val fruitsCsv = getClass.getResource("/data/fruits.csv").getPath
    val fruitSchema = StructType(
      Array(
        StructField("customer", StringType, false),
        StructField("fruit", StringType, false),
        StructField("qty", IntegerType, true),
        StructField("cost", DoubleType, true)
      ))
    spark.read.format("csv").schema(fruitSchema).load(fruitsCsv)
  }

  def readTimeSeriesCsv(day: Int): DataFrame = {
    val timeseriesCsv = getClass.getResource(s"/data/timeseries$day.csv").getPath
    val timeSeriesSchema = StructType(
      Array(
        StructField("date", StringType, false),
        StructField("user", StringType, false),
        StructField("value", StringType, false)
      ))
    spark.read.format("csv").schema(timeSeriesSchema).load(timeseriesCsv)
  }

  def readUrlCsv(): DataFrame = {
    val urlCsv = getClass.getResource(s"/data/urls.csv").getPath
    val urlSchema = StructType(
      Array(
        StructField("domain", StringType, false),
        StructField("url", StringType, false)
      ))
    spark.read.format("csv").schema(urlSchema).load(urlCsv)
  }

  def readRFC4180Csv(spark: SparkSession, input: String): DataFrame = {
    spark.read
      .option("header",true)
      .option("quote","\"")
      .option("escape","\"")
      .csv(input)
  }

  def readFromFruitSeq(): DataFrame = {
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
    val fruitSchema = StructType(
      Array(
        StructField("customer", StringType, false),
        StructField("fruit", StringType, false),
        StructField("qty", IntegerType, true),
        StructField("cost", DoubleType, true)
      ))
    spark.createDataFrame(spark.sparkContext.parallelize(fruitSeq), fruitSchema)
  }

  def listDir(path: String): Unit = {

    val fs = FileSystem.get(URI.create(path), spark.sparkContext.hadoopConfiguration)
    val fileListIterator = fs.listFiles(new Path(path), true)
    while(fileListIterator.hasNext) {
      val fileStatus = fileListIterator.next()
      println(fileStatus)
    }
  }
}

object SparkReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    val reader = new SparkReader(spark)
    val dataPath = getClass.getResource("/data").getPath
    reader.listDir(dataPath)

  }
}