package com.waiyan.ume.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSqlFunctions {

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
    fruitsDF.show

    // Built-In
    val castingAndRounding = false
    val whenOtherwise = false
    val regex = false
    val na = false
    val groupByCollectList = true
    val approxQuantile = false

    // UDFS
    val checkDuplicates = false
    val formatDateUdf = false

    // Casting and Rounding
    if (castingAndRounding) {
      fruitsDF
        .withColumn("cost_int", col("cost").cast("integer"))
        .withColumn("cost_rounded", round(col("cost")).cast("integer"))
        .show
    }

    // Concat, When/Otherwise, Split
    // if name >= 5, domain is abcd.com, xyz.com otherwise
    if (whenOtherwise) {
      fruitsDF
        .withColumn(
          "email",
          concat(col("customer"), when(length(col("customer")) >= 5, lit("@abcd.com")).otherwise("@xyz.com")))
        .withColumn("domain", substring(split(col("email"), "@")(1), 1, 3))
        .show
    }

    // Regex replace
    if (regex) {
      fruitsDF
        .withColumn("fruit_replaced", regexp_replace(col("fruit"), lit("apple"), lit("pineapple")))
        .withColumn("fruit_regex_replaced", regexp_replace(col("fruit"), lit("apple|mango"), lit("banana")))
        .show
    }

    // NA Fill/ Drop
    if (na) {
      fruitsDF
        .withColumn("cost", col("cost").cast("double"))
        .withColumn("qty", col("qty").cast("integer"))
        .na.fill(0.0, Seq("cost"))
        .na.fill(0, Seq("qty"))
        .show

      fruitsDF
        .withColumn("cost", col("cost").cast("double"))
        .na.drop
        .show
    }

    // Flatten, Collect List, Split
    if (groupByCollectList) {
      val flattenDistinct = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)
      fruitsDF
        .groupBy("customer")
        .agg(
          concat_ws(",", flattenDistinct(collect_list(split(col("fruit"), ",")))).alias("fruitList")
        ).show
    }

    // approxQuantile
    if (approxQuantile) {
      val quantiles = fruitsDF.stat.approxQuantile("cost", Array(0.25, 0.5, 0.75), 0.1)
      println(s"Quantile values: ${quantiles.toSeq}")
    }

    // Duplicate Checker
    if (checkDuplicates) {
      val duplicateFruits = SparkSqlUdfs.getDuplicateDF(fruitsDF, Seq("fruit", "customer"))
      duplicateFruits.show
    }

    // Time series data
    if (formatDateUdf) {
      val ts1RawDf = reader.readTimeSeriesCsv(1)
      ts1RawDf.show
      ts1RawDf.withColumn("formattedDate", SparkSqlUdfs.formatTimestampUdf(col("date"))).show
    }
  }

}
