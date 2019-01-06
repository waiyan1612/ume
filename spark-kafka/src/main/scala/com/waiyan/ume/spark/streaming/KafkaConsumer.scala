package com.waiyan.ume.spark.streaming

import java.sql.Date
import com.waiyan.ume.kafka.IKafkaConstants
import com.waiyan.ume.kafka.model.Transaction
import com.waiyan.ume.kafka.serializer.TransactionDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaConsumer {

  // This is a Scala redefinition of com.waiyan.ume.kafka.model.Transaction
  case class Txn(purchasedDate: Date, customerId: Int, productId: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    val txnStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", IKafkaConstants.KAFKA_BROKERS)
      .option("subscribe", IKafkaConstants.TOPIC_NAME)
      .load()

    object TransactionDeserializerWrapper {
      val deser = new TransactionDeserializer
      val scalarize = (t: Transaction) =>
        Txn(
          Date.valueOf(t.getPurchasedDate),
          t.getCustomerId,
          t.getProductId
      )
    }

    val deserializeUdf = udf {
      bytes: Array[Byte] =>
        {
          val t = TransactionDeserializerWrapper.deser.deserialize(IKafkaConstants.TOPIC_NAME, bytes)
          TransactionDeserializerWrapper.scalarize(t)
        }
    }

    // txnStream.show(false)
//    val query = txnStream.writeStream
//      .outputMode("update")
//      .format("console")
//      .start
//    query.awaitTermination

    //import spark.implicits._
    val result = txnStream.select(deserializeUdf(col("value") as "txn"))

    val query1 = result.writeStream
      .outputMode("update")
      .format("console")
      .start
    query1.awaitTermination
  }

}
