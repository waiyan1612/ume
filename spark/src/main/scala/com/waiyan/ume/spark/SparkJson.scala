package com.waiyan.ume.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

object SparkJson {

  case class CompanyContact(emailAddress: String, countryLocationISO36611Alpha2Code: String)

  case class Company(id: Long, name: String, contacts: Seq[CompanyContact])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    testInferSchema(spark)
    testFlatteningSchema(spark)
//    testEscapedJson(spark)

  }

  def testInferSchema(spark: SparkSession): Unit = {
    println("Testing infer schema")
    val path = getClass.getResource("/data/json/json-string-data.json").getPath
    val df = spark.read.json(path)
    import spark.implicits._
    df.select(
      $"user_id",
      extractString("company.reference.timeZone")($"additional_data") as "timeZone"  ,
      get_json_object($"additional_data", "$.company.reference.timeZone") as "timeZone2" ,
      explode_outer(extractCompanyContacts("company.additionalData.companyContact")($"additional_data")) as "companyContacts",
      get_json_object($"additional_data", "$.company.additionalData.companyContact") as "companyContacts2"
    ).show(false)

    val outputPath = "/tmp/schema.json"
    inferSchemaJson(spark, df.select("additional_data"), outputPath)
    println(s"Schema written to $outputPath")
  }

  def testFlatteningSchema(spark: SparkSession): Unit = {
    println("Testing flattening schema")
    val path = getClass.getResource("/data/json/json-string-data.json").getPath
    val df = spark.read.json(path)
    val longSchemaFile = scala.io.Source.fromFile(getClass.getResource("/data/json/total-amount-long-schema.json").getPath)
    val doubleSchemaFile = scala.io.Source.fromFile(getClass.getResource("/data/json/total-amount-double-schema.json").getPath)
    val stringSchemaFile = scala.io.Source.fromFile(getClass.getResource("/data/json/total-amount-string-schema.json").getPath)
    val longSchema = DataType.fromJson(longSchemaFile.mkString).asInstanceOf[StructType]
    val doubleSchema = DataType.fromJson(doubleSchemaFile.mkString).asInstanceOf[StructType]
    val stringSchema =  DataType.fromJson(stringSchemaFile.mkString).asInstanceOf[StructType]

    import spark.implicits._
    val longDf = df.select(from_json($"additional_data", longSchema) as "additional_data")
    longDf.select(flattenSchema(longDf.schema): _*)
      .select($"`additional_data.totalAmount`" as "longSchema")
      .show(false)

    val doubleDf = df.select(from_json($"additional_data", doubleSchema) as "additional_data")
    doubleDf.select(flattenSchema(doubleDf.schema): _*)
      .select($"`additional_data.totalAmount`" as "doubleSchema")
      .show(false)

    val stringDf = df.select(from_json($"additional_data", stringSchema) as "additional_data")
    stringDf.select(flattenSchema(stringDf.schema): _*)
      .select($"`additional_data.totalAmount`" as "stringSchema")
      .show(false)
  }


  def testEscapedJson(spark: SparkSession): Unit = {
    println("Testing escaped JSON")
    val companies = Seq(
      Company(1, "Company 1", Seq(CompanyContact("1@y.com", "1y"), CompanyContact("1@y.biz", "1y"))),
      Company(2, "Company 2", Seq(CompanyContact("2@y.com", "2y"), CompanyContact("2@y.biz", "2y")))
    )

    import spark.implicits._
    val companyDf =  companies.toDF()
    companyDf.show(false)
    companyDf.write.mode("overwrite").parquet("/tmp/parquet/original")

    val companyStringDf = companyDf
      .withColumn("id", $"id".cast("string"))
      .withColumn("name", $"name".cast("string"))
      .withColumn("contacts", to_json($"contacts"))
    companyStringDf.show(false)
    companyStringDf.write.mode("overwrite").parquet("/tmp/parquet/string")

    spark.read.parquet("/tmp/parquet/string")
      .withColumn("id", $"id".cast("int"))
      .withColumn("name", $"name".cast("string"))
      .withColumn("contacts", extractCompanyContacts(".")($"contacts"))
      .show(false)
  }

  def flattenSchema(schema: StructType, optPrefix: Option[String] = None): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (optPrefix.isEmpty) f.name else s"${optPrefix.get}.${f.name}"

      // Handle cases where the name contain dots, backtick needed to escape that.
      val escapedCol = if (optPrefix.isEmpty) f.name else s"${optPrefix.get}.`${f.name}`"

      f.dataType match {
        case st: StructType => flattenSchema(st, Some(colName))
        case _ => Array(col(escapedCol).alias(colName))
      }
    })
  }

  def inferSchemaJson(spark: SparkSession, df: DataFrame, output: String): Unit = {
    import spark.implicits._
    val json = spark
      .read
      .json(df.as[String])
      .schema
      .prettyJson

    println(json)
    import java.io.PrintWriter
    new PrintWriter(output) { write(json); close() }
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

  def extractString(path: String): UserDefinedFunction = udf { jsonString: String => extractJson[String] (jsonString, path) }

  def extractCompanyContacts(path: String): UserDefinedFunction = udf { jsonString: String => extractJson[List[CompanyContact]] (jsonString, path) }

}
