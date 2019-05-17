package com.waiyan.ume.flink.api

import java.net.{ HttpURLConnection, URL }

import com.google.gson.Gson
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Producer {

  val gson = new Gson
  val apiOffset = 500

  def main(args: Array[String]) {

    val key = "API_KEY"
    var cursor = 0
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    var fetchedAll = false
    var carParksAll = Seq[CarPark]()

    while (!fetchedAll) {
      val carParks = getCarParks(cursor, key)
      carParksAll = carParksAll ++ carParks
      fetchedAll = carParks.isEmpty
      cursor += apiOffset
    }

    println("Fetched All")

    import org.apache.flink.api.scala._
    val stream = env.fromCollection(carParksAll)
    stream.print.setParallelism(1)

    env.execute(this.getClass.getName)
  }

  private def getCarParks(cursor: Int, key: String) = {
    try {
      val content = get("http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2?$skip=" + cursor, key)
      gson.fromJson(content, classOf[JsonCarParkResp]).value
    } catch {
      case e @ (_: java.io.IOException) =>
        println("Error occurred in getCarParks", e.getMessage)
        Array[CarPark]()
    }
  }

  @throws(classOf[java.io.IOException])
  private def get(url: String, accountKey: String) = {
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestProperty("AccountKey", accountKey)
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.setRequestMethod("GET")
    val errStream = connection.getErrorStream
    val inputStream = connection.getInputStream
    if (errStream != null) {
      println("Error occurred while making a REST call.")
      println(scala.io.Source.fromInputStream(inputStream).mkString)
    }
    if (inputStream != null) {
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      inputStream.close()
      content
    } else {
      throw new java.io.IOException("Null input stream.")
    }
  }

}
