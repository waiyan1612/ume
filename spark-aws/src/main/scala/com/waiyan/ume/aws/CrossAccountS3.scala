package com.waiyan.ume.aws

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import java.util.concurrent.TimeUnit

import com.waiyan.ume.aws.environment.Environment
import com.waiyan.ume.aws.provider.{LocalAwsSudoProvider, S3Provider}
import software.amazon.awssdk.auth.credentials.{AwsSessionCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.sts.model.{AssumeRoleRequest, Credentials}
import software.amazon.awssdk.services.sts.StsClient

object CrossAccountS3 {

  def getAssumedCreds(stsClient: StsClient, roleArn: String, roleSessionName: String): Credentials = {
    val assumeRoleRequest = AssumeRoleRequest.builder()
      .roleArn(roleArn)
      .roleSessionName(roleSessionName)
      .durationSeconds(900)
      .build()
    stsClient.assumeRole(assumeRoleRequest).credentials()
  }

  def main(args: Array[String]): Unit = {

    val bucketName = "bucket"
    val roleArnToAssume = "arn:aws:iam::ACCOUNT_ID:role/ROLE_A"
    val roleSessionName = "uniqueSessionName"
    val s3KeyPrefix = "subfolder1/"


    // Logging in using own account
    val ownCreds = LocalAwsSudoProvider.get(Environment.STG)
    val ownCredsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(
      ownCreds.accessKeyId(), ownCreds.secretAccessKey(), ownCreds.sessionToken()
    ))

    // Assuming the role from another account
    val stsClient = StsClient.builder().credentialsProvider(ownCredsProvider).build()
    val assumedCreds = getAssumedCreds(stsClient, roleArnToAssume, roleSessionName)
    val expiry = assumedCreds.expiration()
    println("Expiring in ", expiry)

    // Building s3Client using this credentials
    var s3Provider = new S3Provider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
      assumedCreds.accessKeyId(), assumedCreds.secretAccessKey(), assumedCreds.sessionToken()
    )))
    var i = 0

    while(true) {

      println(new Date())
      s3Provider.writeString(bucketName, s3KeyPrefix + s"${i+=1}.txt", "hello world")
      TimeUnit.MINUTES.sleep(5)

      if(Instant.now().isAfter(expiry.minus(3, ChronoUnit.MINUTES))) {
        // renew credentials
        println("Renewing Credentials")
        val assumedCreds = getAssumedCreds(stsClient, roleArnToAssume, roleSessionName)
        s3Provider = new S3Provider(StaticCredentialsProvider.create(AwsSessionCredentials.create(
          assumedCreds.accessKeyId(), assumedCreds.secretAccessKey(), assumedCreds.sessionToken()
        )))
        val expiry = assumedCreds.expiration()
        println("Expiring in ", expiry)

      }
    }

  }

}
