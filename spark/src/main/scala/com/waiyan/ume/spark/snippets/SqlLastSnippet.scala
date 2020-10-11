package com.waiyan.ume.spark.snippets

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object SqlLastSnippet {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()
    import spark.implicits._

    // Simulating the dataset
    // We have an id which has a different balance for different dates.
    // If there is no change in balance, it is null.
    // We want to infer the actual balance for these null values.
    val rawData = Seq(
      Row("1", "01-01-2020", 100),
      Row("1", "02-01-2020", 200),
      Row("2", "01-01-2020", -100),     // test out-of-order id
      Row("2", "02-01-2020", null),
      Row("1", "03-01-2020", null),
      Row("1", "05-01-2020", 500),      // test out-of-order date,
      Row("1", "04-01-2020", null),
      Row("1", "06-01-2020", null)     // test value is 500 since we have data for 05-01-2020
    )

    val schema = StructType(
      Array(
        StructField("id", StringType, nullable=false),
        StructField("date", StringType, nullable=false),
        StructField("balance", IntegerType, nullable=true)
      ))

    // Reading into data frame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rawData), schema)

    // Creating a window, order by date ASC partitionBy id
    val window = Window.partitionBy($"id").orderBy($"date").rowsBetween(Window.unboundedPreceding, -1)

    // For each id, get the last (ordered by date) non null value
    df.withColumn("inferred_balance", coalesce($"balance", last($"balance", ignoreNulls=true).over(window))).show(false)
  }

}
