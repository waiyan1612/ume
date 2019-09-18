package com.waiyan.ume.spark.datastore

import java.util.Properties

import com.waiyan.ume.spark.SparkReader
import org.apache.spark.sql.SparkSession

object SparkJDBC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    // Reading from Seq or CSV with a schema
    val reader = new SparkReader(spark)
    val fruitsDF = reader
      .readFruitsCsv()
      .withColumnRenamed("customer", "customer_id")
      .withColumnRenamed("fruit", "fruit_id")

    fruitsDF.show

    val url: String = "jdbc:postgresql://localhost/ume"
    val tableName: String = "fruits"
    val properties = new Properties()

    properties.setProperty("user", "postgres")
    properties.setProperty("password", "postgres")
    properties.setProperty("batchsize", "1000") //try 10000
    properties.put("driver", "org.postgresql.Driver")
    fruitsDF.write.mode("append").jdbc(url, tableName, properties)

  }
}
