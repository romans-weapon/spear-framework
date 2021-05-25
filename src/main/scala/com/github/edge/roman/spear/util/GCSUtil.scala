package com.github.edge.roman.spear.util

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.Channels
import scala.util.{Failure, Success, Try}

class GCSUtil {

  var storage: Storage = _
  var bucket_name: String = _

  def configureClient(configMap: Map[String, String]): Unit = {
    try {
      bucket_name = configMap("bucketName")
      val projectId: String = configMap("projectId")
      val gcsAuthKeyPath: String = configMap("gcsAuthKeyPath")
      storage = StorageOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(GoogleCredentials.fromStream(
          new FileInputStream(gcsAuthKeyPath))).build().getService
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def downloadFile(remote: String): InputStream = {
    try {
      val reader: ReadChannel = storage.reader(bucket_name, remote)
      Channels.newInputStream(reader)
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def uploadFile(remote: String, file: File): Unit = {
    try {
      val blobId = BlobId.of(bucket_name, remote)
      val storage = StorageOptions.getDefaultInstance.getService
      storage.createFrom(BlobInfo.newBuilder(blobId).build(), new FileInputStream(file))
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }


  def uploadFile(remote: String, size: Long, fileStream: InputStream): Unit = {
    try {
      val blobId = BlobId.of(bucket_name, remote)
      val storage = StorageOptions.getDefaultInstance.getService
      storage.createFrom(BlobInfo.newBuilder(blobId).build(), fileStream)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def getSize(remote: String): Option[Long] = {

    Try {
      val blobId = BlobId.of(bucket_name, remote)
      val size = storage.get(blobId).getSize
      size
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }
}
