package com.waiyan.ume.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

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

    case class Demo(
      window: Boolean = false, // window
      agg: Boolean = false, // cast, round
      sql: Boolean = false, // raw sql on table/view
      cast: Boolean = false, // cast, round
      when: Boolean = false, // when-otherwise, concat, substring, split
      explode: Boolean = true, // explode, explode_outer
      pivot: Boolean = false,
      regex: Boolean = false, // regexp_replace
      na: Boolean = false, // na functions
      groupByCollectList: Boolean = false, // flattenDistinct, collect_list
      approxQuantile: Boolean = false, // approxQuantile
      checkDuplicates: Boolean = false,
      formatDateUdf: Boolean = false
    )
    val demo = Demo()

    if (demo.window) {
      val w = Window.partitionBy(col("customer")).orderBy(col("qty").desc_nulls_last)
      fruitsDF.withColumn("seq", row_number.over(w)).show
    }

    // operations on columns ignore nulls
    if (demo.agg)
      fruitsDF
        .select(
          avg("qty"),
          sum("qty"),
          count("qty"),
          count("*")
        ).show

    if (demo.sql) {
      fruitsDF.createOrReplaceTempView("fruits")
      spark.sql("SELECT count(*) AS count FROM fruits").show
    }

    // Casting and Rounding
    if (demo.cast) {
      fruitsDF
        .withColumn("cost_int", col("cost").cast("integer"))
        .withColumn("cost_rounded", round(col("cost")).cast("integer"))
        .withColumn("null_col", lit(null).cast(StringType))
        .show
    }

    // if name >= 5, domain is abcd.com, xyz.com otherwise
    if (demo.when) {
      fruitsDF
        .withColumn(
          "email",
          concat(col("customer"), when(length(col("customer")) >= 5, lit("@abcd.com")).otherwise("@xyz.com")))
        .withColumn("domain", substring(split(col("email"), "@")(1), 1, 3))
        .show
    }

    if (demo.explode) {
      val DAYS = Seq("SUN", "MON")
      // alice will have nulls in dayList
      val daysDf =
        fruitsDF.withColumn("dayList", when(col("customer") === "alice", typedLit(null)).otherwise(typedLit(DAYS)))
      daysDf.show
      // alice is gone
      daysDf.withColumn("day", explode(col("dayList"))).show
      // How to keep alice alive
      daysDf.withColumn("day", explode_outer(col("dayList"))).show
    }

    if (demo.pivot) {
      fruitsDF.groupBy("customer").pivot("fruit").agg(sum("qty")).show
    }

    // Regex replace
    if (demo.regex) {
      fruitsDF
        .withColumn("fruit_replaced", regexp_replace(col("fruit"), lit("apple"), lit("pineapple")))
        .withColumn("fruit_regex_replaced", regexp_replace(col("fruit"), lit("apple|mango"), lit("banana")))
        .show
    }

    // NA Fill/ Drop
    if (demo.na) {
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

    if (demo.groupByCollectList) {
      val flattenDistinct = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)
      fruitsDF
        .groupBy("customer")
        .agg(
          concat_ws(",", flattenDistinct(collect_list(split(col("fruit"), ",")))).alias("fruitList")
        ).show
    }

    if (demo.approxQuantile) {
      val quantiles = fruitsDF.stat.approxQuantile("cost", Array(0.25, 0.5, 0.75), 0.1)
      println(s"Quantile values: ${quantiles.toSeq}")
    }

    // UDFs
    if (demo.checkDuplicates) {
      val duplicateFruits = SparkSqlUdfs.getDuplicateDF(fruitsDF, Seq("fruit", "customer"))
      duplicateFruits.show
    }

    if (demo.formatDateUdf) {
      val ts1RawDf = reader.readTimeSeriesCsv(1)
      ts1RawDf.show
      ts1RawDf.withColumn("formattedDate", SparkSqlUdfs.formatTimestampUdf(col("date"))).show
    }
  }

}
