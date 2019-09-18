package com.waiyan.ume.spark.config

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

import scala.io.Source

sealed trait MyAdt
case class AdtA(a: String) extends MyAdt
case class AdtB(b: Int) extends MyAdt

final case class Port(value: Int) extends AnyVal

case class UmeConfig(
  boolean: Boolean,
  port: Port,
  adt: MyAdt,
  list: List[Double],
  map: Map[String, String],
  option: Option[String]
)

object ConfReader {

  private def parseConfig[T](config: ConfigReader.Result[T]): T = {
    if (config.isLeft) {
      throw new Exception(config.left.get.toList.mkString(System.lineSeparator))
    } else {
      config.right.get
    }
  }

  private def readConfigFile(lines: Iterator[String]) = {
    import pureconfig.generic.auto._
    val config = ConfigFactory.parseString(lines.mkString("\n"))
    parseConfig(pureconfig.loadConfig[com.waiyan.ume.spark.config.UmeConfig](config))
  }

  private def fromResource(path: String): File = {
    new File(getClass.getClassLoader.getResource(path).getPath)
  }

  // Read the contents of a given filename and return an iterator (line by line) of the file contents
  // This is essentially a function being passed as an argument
  private def init(readFile: String => Iterator[String]): UmeConfig = {

    try {
      readConfigFile(readFile("wrong-config.conf"))
    } catch {
      case e: Exception => println(e)
    }

    val contents: Iterator[String] = readFile("right-config.conf")
    readConfigFile(contents)
  }

  def from(dir: String, spark: Option[SparkSession] = None): UmeConfig = {
    if (spark.isDefined) {
      init(x => spark.get.sparkContext.textFile(s"$dir/$x").collect.iterator)
    } else {
      init(x => Source.fromFile(fromResource(s"$dir/$x")).getLines)
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    val pureConfig = ConfReader.from("conf")
    println(pureConfig)

  }
}
