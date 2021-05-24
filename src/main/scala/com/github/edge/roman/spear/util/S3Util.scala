package com.github.edge.roman.spear.util

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.{File, InputStream}
import java.util

class S3Util {

  var amazonS3Client: AmazonS3 = _
  var bucket_name: String = _

  def configureClient(configMap: Map[String, String]): Unit = {
    try {
      bucket_name = configMap("bucket_name")
      val accessKey: String = configMap("access_key")
      val secretAccessKey: String = configMap("secret_key")
      val region: String = configMap("region")
      val awsCredentials = new BasicAWSCredentials(accessKey, secretAccessKey)
      amazonS3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).withRegion(region).build()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def downloadFile(remote: String): InputStream = {
    try {
      val s3Object: S3Object = amazonS3Client.getObject(bucket_name, remote)
      s3Object.getObjectContent
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def uploadFile(remote: String, file: File): Unit = {
    try {
      amazonS3Client.putObject(bucket_name, remote, file)
    } catch {
      case exception: Exception =>exception.printStackTrace()
    }
  }


  def uploadFile(remote: String, size: Long, fileStream: InputStream): Unit = {
    try {
      val metadata: ObjectMetadata = new ObjectMetadata()
      metadata.setContentLength(size)
      amazonS3Client.putObject(bucket_name, remote, fileStream, metadata)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def getSize(remote: String): Long = {
    var size: Long = 0L
    try {
      val listObjectsRequest: ListObjectsRequest = new ListObjectsRequest().withBucketName(bucket_name).withPrefix(remote).withDelimiter("/")
      val objects: ObjectListing = amazonS3Client.listObjects(listObjectsRequest)
      val summaries: util.List[S3ObjectSummary] = objects.getObjectSummaries
      size = summaries.get(0).getSize
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
    size
  }
}
