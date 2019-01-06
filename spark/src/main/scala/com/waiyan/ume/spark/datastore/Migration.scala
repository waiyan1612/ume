package com.waiyan.ume.spark.datastore

import org.flywaydb.core.Flyway
import scopt.OptionParser

object Migration {

  case class ConnectionInfo(
    host: String = "localhost",
    database: String = "ume",
    user: String = "postgres",
    password: String = "postgres")

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getName
    val parser = new OptionParser[ConnectionInfo](appName) {
      opt[String]("host").text("host").required.action((x, c) => c.copy(host = x))
      opt[String]("database").text("database").required.action((x, c) => c.copy(database = x))
      opt[String]("user").text("user").required.action((x, c) => c.copy(user = x))
      opt[String]("password").text("password").required.action((x, c) => c.copy(password = x))
    }

    val conn = ConnectionInfo() //parser.parse(args, ConnectionInfo()).head
    val jdbcUrl = s"jdbc:postgresql://${conn.host}/${conn.database}"
    val flyway = Flyway.configure().dataSource(jdbcUrl, conn.user, conn.password).schemas("public").load()
    flyway.migrate()
    println("Migration Complete")

  }
}
