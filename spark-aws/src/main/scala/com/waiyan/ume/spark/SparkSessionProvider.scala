package com.waiyan.ume.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.sts.model.Credentials

object SparkSessionProvider {

  def get(creds: Credentials): SparkSession = {

    val sparkConf = new SparkConf()
      .setAppName("profiler")

      sparkConf
        .set("spark.hadoop.fs.s3a.access.key", creds.accessKeyId)
        .set("spark.hadoop.fs.s3a.secret.key", creds.secretAccessKey)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.session.timeZone", "UTC")

      if (creds.sessionToken != null && creds.sessionToken.nonEmpty) {
        sparkConf
          .set("spark.hadoop.fs.s3a.session.token", creds.sessionToken)
          .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      }

    SparkSession
      .builder
      .master("local") // or yarn
      .config(sparkConf)
      .getOrCreate()
  }

}
