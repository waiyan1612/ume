package com.waiyan.ume.spark.snippets

import org.apache.spark.sql.SparkSession


object MultipleCharDelimiterSnippet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val customersCsv = getClass.getResource("/data/customers.csv").getPath
    spark.read.csv(customersCsv).show()
    spark.read.option("delimiter", ",\t").csv(customersCsv).show()
  }
}
