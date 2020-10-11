package com.waiyan.ume.spark.snippets

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UdfSnippet {

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

  def formatDecimal(valueToFormat: Double, scale: String): String = s"%1.${scale}f".format(valueToFormat)

  def formatDecimalUdf: UserDefinedFunction = udf {
    (valueToFormat: Double, precisionString: String) => formatDecimal(valueToFormat, precisionString)
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

  def extractJson[T: Manifest](jsonString:String, path: String):  Option[T] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: DefaultFormats.type = DefaultFormats
    try {
      val json = parse(jsonString)
      path.split('.').foldLeft(json)({ case (acc, node) => acc \ node }).extractOpt[T]
    } catch {
      case e: Exception => println(e.getMessage)
        None
    }
  }

  def extractString(path: String) = udf { jsonString: String => extractJson[String] (jsonString, path) }

}
