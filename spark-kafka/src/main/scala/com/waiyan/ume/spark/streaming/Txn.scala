package com.waiyan.ume.spark.streaming

/**
 * This is a Scala redefinition of com.waiyan.ume.kafka.model.Transaction
 */
case class Txn(dt: java.sql.Timestamp, cId: Int, pId: String)
