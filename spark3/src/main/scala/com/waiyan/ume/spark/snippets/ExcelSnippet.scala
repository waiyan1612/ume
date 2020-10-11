package com.waiyan.ume.spark.snippets

import org.apache.spark.sql.SparkSession


object ExcelSnippet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .getOrCreate()

    val multipleSheetsExcel = getClass.getResource("/data/multiple_sheets.xlsx").getPath
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", true)
      .option("dataAddress", "'Second Sheet'!A1")
      .load(multipleSheetsExcel)
      .show()

  }

}
