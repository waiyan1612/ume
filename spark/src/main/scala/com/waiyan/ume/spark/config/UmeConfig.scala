package com.waiyan.ume.spark.config

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
