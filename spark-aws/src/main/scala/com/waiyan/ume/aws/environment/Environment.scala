package com.waiyan.ume.aws.environment

object Environment {
  val DEV = "dev"
  val PROD = "prod"
  val STG = "stg"

  def getProfile(environment: String): String = {
    null
  }

  def getDataLakePath(environment: String): String = {
    null
  }
}
