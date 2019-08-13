package com.waiyan.ume.spark

import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction

object SparkSqlUdfs {

  object Formatters {
    val fromFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val toFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
  }

  def formatTimestamp(timestamp: String): String = {
    LocalDate.parse(timestamp, Formatters.fromFormatter).format(Formatters.toFormatter)
  }

  // Date format UDF
  val formatTimestampUdf: UserDefinedFunction = udf {
    timestamp: String =>
      formatTimestamp(timestamp)
  }

  // Use this function to search for duplicate rows
  import org.apache.spark.sql.DataFrame
  def getDuplicateDF(df: DataFrame, primaryKeys: Seq[String]): DataFrame = {
    val columns = primaryKeys.map(x => col(x))
    val countDf = df
      .select(columns: _*).withColumn("count", lit(1))
      .groupBy(columns: _*).agg(sum("count").as("count"))
      .filter(col("count") > 1)
    df.join(countDf, primaryKeys, "inner")
  }
}
