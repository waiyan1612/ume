package com.waiyan.ume.aws.provider

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CommonPrefix, ListObjectsV2Request, PutObjectRequest}

import scala.collection.mutable

class S3Provider(credsProvider: AwsCredentialsProvider) {

  private val region: Region = Region.AP_SOUTHEAST_1
  private val s3Client = S3Client.builder
      .credentialsProvider(credsProvider)
      .region(region).build

  def writeString(bucket: String, key: String, content: String): Unit = {
    val putObjectRequest = PutObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build()
    s3Client.putObject(putObjectRequest, RequestBody.fromString(content))
  }

  def getLevel2Folders(bucket: String): mutable.Buffer[List[String]] = {
    val rootLevelFolders = getPrefixes(bucket, "")
    rootLevelFolders.map(folder => {
      getPrefixes(bucket, folder.prefix()).map(s"s3://$bucket/" + _.prefix).toList
    })
  }

  private def getPrefixes(bucket: String, prefix: String): mutable.Buffer[CommonPrefix]= {
    val req = ListObjectsV2Request.builder()
      .bucket(bucket)
      .prefix(prefix)
      .delimiter("/")
      .build()

    import scala.collection.JavaConverters._
    s3Client.listObjectsV2(req).commonPrefixes().asScala
  }

}
