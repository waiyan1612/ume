package com.waiyan.ume.spark

import org.apache.spark.sql.{ Column, DataFrame, Row, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
      rdd: Boolean = false,
      window: Boolean = false, // window
      agg: Boolean = false, // cast, round
      sql: Boolean = false, // raw sql on table/view
      cast: Boolean = false, // cast, round
      when: Boolean = false, // when-otherwise, concat, substring, split
      explode: Boolean = true, // explode, explode_outer
      isin: Boolean = false,
      nullCheck: Boolean = false,
      pivot: Boolean = false,
      regex: Boolean = false, // regexp_replace
      na: Boolean = false, // na functions
      join: Boolean = false,
      groupByCollectList: Boolean = false, // flattenDistinct, collect_list
      approxQuantile: Boolean = false, // approxQuantile
      mergeSchema: Boolean = false,
      checkDuplicates: Boolean = false,
      formatDateUdf: Boolean = false
    )
    val demo = Demo()

    if (demo.rdd) {
      fruitsDF.show
      val schema = StructType(fruitsDF.schema.fields.toList ++ List(StructField("rank", LongType, false)))
      val fruitsRdd = fruitsDF.rdd
        .sortBy(_.getAs[Int]("qty"), false) // get field by name
        .zipWithIndex() // zip with index
        .map(x => Row.fromSeq(x._1.toSeq ++ Seq[Long](x._2))) // merge tuples
      val fruitsRddDf = spark.createDataFrame(fruitsRdd, schema) // recreate dataframe
      fruitsRddDf.show
    }

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

    if (demo.isin) {
      val DAYS = Seq("SUN", "MON")
      val daysDf = fruitsDF
        .withColumn("dayList", when(col("customer") === "alice", typedLit(null)).otherwise(typedLit(DAYS)))
        .withColumn("day", explode_outer(col("dayList")))

      // isin doesn't keep NULLS
      val SUNDAY = Seq("SUN")
      daysDf.filter(col("day").isin(SUNDAY: _*)).show()
      // Alternative without defining the list
      daysDf.filter(col("day").isin("SUN")).show
    }

    if (demo.nullCheck) {
      val nullDf = fruitsDF.withColumn(
        "title",
        when(col("customer") === "alice", lit("Ms"))
          .when(col("customer") === "bob", lit("Mr"))
          .otherwise(typedLit(null)))

      def filter(condition: Option[Column], seq: Int, desc: String, note: String): DataFrame = {
        val df = nullDf
          .select("customer", "title", "cost").distinct
          .withColumn("desc", lit(desc))
          .withColumn("note", lit(note))
          .withColumn("seq", lit(seq))
        if (condition.isEmpty) df else df.filter(condition.get)
      }

      val c0 = filter(None, 0, "", "original")
      val c1 = filter(Some(col("title") === "Mr"), 1, " title === Mr ", "")
      val c2 = filter(Some(col("title") =!= "Mr"), 2, " title =!= Mr ", "nulls are gone (carol)")
      val c3 = filter(Some(!(col("title") <=> "Mr")), 3, " !title <=> Mr ", "nulls are kept")
      val c4 = filter(Some(col("cost") === 0.5), 4, " cost === 0.5 ", "")
      val c5 = filter(Some(col("cost") =!= 0.5), 5, " cost =!= 0.5 ", "nulls are gone")
      val c6 = filter(Some(!(col("cost") <=> 0.5)), 6, " !cost <=> 0.5 ", "nulls are kept")
      c0.union(c1).union(c2).union(c3)
        .union(c4).union(c5).union(c6)
        .sort("seq", "customer").show(50, false)
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

    if(demo.join) {
      val customersDf = reader.readCustomersCsv()
      val fruitsDf = reader.readFruitsCsv()
      // left join
      fruitsDf.join(customersDf, col("name") === col("customer"), "left").show
      // filter left before join
      fruitsDf.filter(col("customer") === "alice").join(customersDf, col("name") === col("customer"), "left").show
      // join on left is alice and keys match
      fruitsDf.join(customersDf, col("customer") === "alice" && col("name") === col("customer"), "left").show
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

    if (demo.mergeSchema) {
      fruitsDF.coalesce(1).write.mode("overwrite").parquet("/tmp/quickTest/folderId=1")
      fruitsDF.withColumn("myNewColumn", lit(2)).coalesce(1).write.mode("overwrite").parquet("/tmp/quickTest/folderId=2")
      spark.read.parquet("/tmp/quickTest").show(false)
      spark.read.parquet("/tmp/quickTest/folderId=2").show(false)
      spark.read.option("mergeSchema", true).parquet("/tmp/quickTest").show(false)
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
