package com.waiyan.ume.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{ DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.functions._

object SparkDsDf {

  case class Fruit(customer: String, fruit: String, qty: java.lang.Integer, cost: java.lang.Double)
  case class FruitWindow(customer: String, fruit: String, qty: java.lang.Integer, cost: java.lang.Double, seq: Int)
  case class Customer(name: String, country: String)
  case class FruitCustomer(
    customer: String,
    country: String,
    fruit: String,
    qty: java.lang.Integer,
    cost: java.lang.Double)

  def asDataFrames(fruitsDf: DataFrame, customersDf: DataFrame): Unit = {
    fruitsDf.join(customersDf, col("name") === col("customer"), "left").show
  }

  def asDatasets(fruitsDf: Dataset[Fruit], customersDf: Dataset[Customer]): Unit = {
    import fruitsDf.sparkSession.implicits._
    val joinWith = fruitsDf.joinWith(customersDf, fruitsDf("customer") === customersDf("name"), "left")
    joinWith.show

    joinWith.map {
      case (f, c) => FruitCustomer(f.customer, c.country, f.fruit, f.qty, f.cost)
    }.show
  }

  // window and foldLeft operations
  def windowFoldLeft(fruitsDf: DataFrame): Unit = {
    val w = Window.partitionBy(col("customer")).orderBy(col("qty").desc_nulls_last)
    val windowDf = fruitsDf.withColumn("seq", row_number.over(w))
    windowDf.show

    import fruitsDf.sparkSession.implicits._
    val windowDs = windowDf.as[FruitWindow]
    val dfs = (1 to 3).map(
      i => windowDs.filter(x => x.seq == i).map(x => (x.seq, x.customer, x.fruit))
    )

    dfs(0).show
    dfs(1).show
    dfs(2).show

    val unioned = dfs.tail.foldLeft(dfs.head) {
      (a, b) =>
        a.union(b)
    }
    unioned.show
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", true)
      .getOrCreate()

    val reader = new SparkReader(spark)
    val customersDF = reader.readCustomersCsv()
    val fruitsDF = reader.readFruitsCsv()

    windowFoldLeft(fruitsDF)

    asDataFrames(fruitsDF, customersDF)

    import spark.implicits._
    asDatasets(fruitsDF.as[Fruit], customersDF.as[Customer])
  }
}
