package com.waiyan.ume.aws.provider

import com.waiyan.ume.aws.environment.Environment
import software.amazon.awssdk.services.sts.model.Credentials

import scala.sys.process.Process

object LocalAwsSudoProvider {

  def get(environment: String): Credentials = {
    val profile = Environment.getProfile(environment)
    val awsEnvVars = Process(s"/anaconda3/envs/awsudo/bin/awsudo -u $profile -- env").lineStream.filter(_.startsWith("AWS_")).toList
    val accessKeyId = awsEnvVars.filter(_.startsWith("AWS_ACCESS_KEY_ID=")).map(x => x.substring(x.indexOf("=") + 1)).head
    val secretAccessKey = awsEnvVars.filter(_.startsWith("AWS_SECRET_ACCESS_KEY=")).map(x => x.substring(x.indexOf("=") + 1)).head
    val sessionToken = awsEnvVars.filter(_.startsWith("AWS_SESSION_TOKEN=")).map(x => x.substring(x.indexOf("=") + 1)).head
    Credentials.builder()
      .accessKeyId(accessKeyId)
      .secretAccessKey(secretAccessKey)
      .sessionToken(sessionToken)
      .build()
  }

}
