package com.waiyan.ume.spark.streaming

import com.waiyan.ume.kafka.IKafkaConstants
import com.waiyan.ume.kafka.model.Transaction
import com.waiyan.ume.kafka.serializer.TransactionDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.concurrent.TimeUnit

// This is a Scala redefinition of com.waiyan.ume.kafka.model.Transaction
case class Txn(dt: java.sql.Timestamp, cId: Int, pId: String)

object KafkaConsumer {

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
        Txn(java.sql.Timestamp.valueOf(t.getPurchasedDate), t.getCustomerId, t.getProductId)
    }

    val deserializeUdf = udf {
      bytes: Array[Byte] =>
        {
          val t = TransactionDeserializerWrapper.deser.deserialize(IKafkaConstants.TOPIC_NAME, bytes)
          TransactionDeserializerWrapper.scalarize(t)
        }
    }

    // Preview raw kafka stream
    // val rawStream = txnStream.writeStream.outputMode("update").format("console").start
    // rawStream.stop()

    import spark.implicits._
    val txnDf = txnStream
      .select(deserializeUdf(col("value")).as("txn"))
      .select("txn.dt", "txn.cId", "txn.pId").as[Txn]

    // Trigger.ProcessingTime can be used to how fast you want to consume the incoming data
    // By default, spark remembers all the windows forever and waits for the late events forever
    val txnNonWatermark = txnDf
      .groupBy(window(col("dt"), "4 minutes", "2 minutes"))
      .count

    val q1 = txnNonWatermark.writeStream
      .option("truncate", false).outputMode("update").format("console")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES)).start

    q1.stop

    // Wait late events for 5 min (an entry is intentionally delayed by 1-9 min by Kafka Producer)
    // Data comes in every 30 s, perform operations within 4 minute windows, sliding every 2 minutes

    /*

      Kafka Producer  = 30 s
      Trigger         = 1 min
      Window          = 4 min, Sliding window = 2 min
      Watermark       = 5 min

      Since 10 records are produced every 30 seconds, we will see 20 records per trigger.
      Records are dropped if (watermark is older than END window)

      +---------+---------------------------------------------+-----+-----+---------+
      |triggered|window                                       |count|data |watermark|
      +---------+---------------------------------------------+-----+-----+---------+
      |11:20    |[2019-05-09 11:10:00.0,2019-05-09 11:14:00.0]|92   |11:19|11:14
      |         |[2019-05-09 11:16:00.0,2019-05-09 11:20:00.0]|72   |
      |         |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|36   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:21    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|56   |11:20|11:15
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|20   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:22    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|76   |11:21|11:16
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|40   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:23    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|78   |11:22|11:17
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|60   |
      |         |[2019-05-09 11:22:00.0,2019-05-09 11:26:00.0]|18   | // 2 delayed records go to first 18-22 window
      +---------+---------------------------------------------+-----+-----+---------+
      |11:24    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|80   |11:23|11:18
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|80   |
      |         |[2019-05-09 11:22:00.0,2019-05-09 11:26:00.0]|36   | // another 2
      +---------+---------------------------------------------+-----+-----+---------+
      |11:25    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|82   |11:24|11:19
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|82   |
      |         |[2019-05-09 11:22:00.0,2019-05-09 11:26:00.0]|54   |
      |         |[2019-05-09 11:24:00.0,2019-05-09 11:28:00.0]|18   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:26    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|84   |11:25|11:20
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|84   |
      |         |[2019-05-09 11:22:00.0,2019-05-09 11:26:00.0]|72   |
      |         |[2019-05-09 11:24:00.0,2019-05-09 11:28:00.0]|36   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:27    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|86   |11:26|11:21
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|86   |
      |         |[2019-05-09 11:24:00.0,2019-05-09 11:28:00.0]|54   |
      |         |[2019-05-09 11:26:00.0,2019-05-09 11:30:00.0]|18   |
      +---------+---------------------------------------------+-----+-----+---------+
      |11:28    |[2019-05-09 11:18:00.0,2019-05-09 11:22:00.0]|88   |11:27|11:22
      |         |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|88   |
      |         |[2019-05-09 11:24:00.0,2019-05-09 11:28:00.0]|72   |
      |         |[2019-05-09 11:26:00.0,2019-05-09 11:30:00.0]|36   | // notice 18-22 window is still here
      +---------+---------------------------------------------+-----+-----+---------+
      |11:29    |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|90   |11:28|11:23
      |         |[2019-05-09 11:26:00.0,2019-05-09 11:30:00.0]|54   |
      |         |[2019-05-09 11:28:00.0,2019-05-09 11:32:00.0]|18   | // notice 18-22 window is gone
      +---------+---------------------------------------------+-----+-----+---------+
      |11:30    |[2019-05-09 11:20:00.0,2019-05-09 11:24:00.0]|92   |11:29|11:24
      |         |[2019-05-09 11:26:00.0,2019-05-09 11:30:00.0]|72   |
      |         |[2019-05-09 11:28:00.0,2019-05-09 11:32:00.0]|36   |
      +---------+---------------------------------------------+-----+-----+---------+

     */

    val txnWatermark = txnDf
      .withWatermark("dt", "5 minutes")
      .groupBy(window(col("dt"), "4 minutes", "2 minutes"))
      .count

    val q2 = txnWatermark.writeStream
      .option("truncate", false).outputMode("update").format("console")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES)).start

    q2.stop

    /*

      File system based sinks only accept "append" mode.
      Records are written when watermark is older than END window

      +---------+---------------------------------------------+-----+---------+
      |triggered|window                                       |count|watermark|
      +---------+---------------------------------------------+-----+---------+
      |13:41    |[2019-05-09 13:30:00.0,2019-05-09 13:34:00.0]|92   |13:35    |
      +---------+---------------------------------------------+-----+---------+
      |13:43    |[2019-05-09 13:32:00.0,2019-05-09 13:36:00.0]|72   |13:37    |
      +---------+---------------------------------------------+-----+---------+
      |13:45    |[2019-05-09 13:34:00.0,2019-05-09 13:38:00.0]|72   |13:39    |
      +---------+---------------------------------------------+-----+---------+
      |13:47    |[2019-05-09 13:36:00.0,2019-05-09 13:40:00.0]|72   |13:41    |
      +---------+---------------------------------------------+-----+---------+
      |13:49    |[2019-05-09 13:28:00.0,2019-05-09 13:32:00.0]|90   |13:43    |
      +---------+---------------------------------------------+-----+---------+
      |13:51    |[2019-05-09 13:40:00.0,2019-05-09 13:44:00.0]|92   |13:45    |
      +---------+---------------------------------------------+-----+---------+

     */

    val q3 = txnWatermark.writeStream
      .option("truncate", false).outputMode("append").format("console")
      .trigger(Trigger.ProcessingTime(2, TimeUnit.MINUTES)).start

    q3.stop

    // Write to parquet with checkpoints
    val customerDf = txnDf
      .withWatermark("dt", "4 minutes")
      .groupBy(window(col("dt"), "2 minutes"), col("cId"))
      .agg(count("pId").as("itemsBought"))

    val customerQ = customerDf.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2, TimeUnit.MINUTES))
      .format("parquet")
      .option("path", "ume-spark-kafka")
      .option("checkpointLocation", "ume-spark-kafka-checkpoint")
      .start

    spark.streams.awaitAnyTermination
  }

}
