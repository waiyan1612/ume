package com.waiyan.ume.spark

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.SparkException

class SparkSpec extends FlatSpec with Matchers with DatasetSuiteBase {

  val testSeq = Seq(Row("A", "Apple"), Row("B", "Banana"))

  val testSchema = StructType(
    Array(
      StructField("alias", StringType, false),
      StructField("name", StringType, false)
    ))
  var df: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    df = spark.createDataFrame(spark.sparkContext.parallelize(testSeq), testSchema)
  }

  "DataFrames" should "be identical" in {
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(testSeq), testSchema)
    val actualDf = spark.createDataFrame(spark.sparkContext.parallelize(testSeq), testSchema)
    assertDataFrameEquals(expectedDf, actualDf)
  }

  "Accessing a non existent key in a map in scala" should "throw NoSuchElementException" in {
    val testMap = Map("A" -> 1, "C" -> 2)
    assertThrows[NoSuchElementException] {
      testMap("B")
    }
  }

  "Accessing a non existent key in a map in spark" should "throw NoSuchElementException" in {
    import spark.implicits._
    val thrown = intercept[SparkException] {
      val testMap = Map("A" -> 1, "C" -> 2)
      df.map(row => { testMap(row(0).toString) }).show
    }

    assert(thrown.getCause match {
      case p: NoSuchElementException => true
      case _ => false
    })

  }

}
