package com.waiyan.ume.spark

import com.waiyan.ume.aws.provider.{LocalAwsSudoProvider, S3Provider}
import com.waiyan.ume.aws.environment.Environment
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials, StaticCredentialsProvider}

object Coordinator {

  def coordinate(environment: String): Unit = {

    val datalake = Environment.getDataLakePath(environment)
    val ownCreds = LocalAwsSudoProvider.get(environment)
    val ownCredsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(
      ownCreds.accessKeyId(), ownCreds.secretAccessKey(), ownCreds.sessionToken()
    ))

    val s3Provider = new S3Provider(ownCredsProvider)
    val list = s3Provider.getLevel2Folders(datalake)
    list.foreach(println)

    val spark = SparkSessionProvider.get(ownCreds)
  }


  def main(args: Array[String]): Unit = {

    val environment = Environment.PROD
//    val environment = Environment.DEV
    coordinate(environment)

  }


}
