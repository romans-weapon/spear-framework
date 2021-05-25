package com.github.edge.roman.spear.util

import com.google.cloud.storage.BlobId
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer}

import java.io.{File, FileInputStream, InputStream}
import scala.util.{Failure, Success, Try}

class ADLSUtil {

  var containerName: String = _
  val defaultEndpointsProtocol: String = "https"
  var container: CloudBlobContainer = _
  var blobClient: CloudBlobClient = _

  def configureClient(configMap: Map[String, String]): Unit = {
    try {
      containerName = configMap("containerName")
      val accountName: String = configMap("accountName")
      val accountKey: String = configMap("accountKey")
      val storageConnectionString = "DefaultEndpointsProtocol=" + defaultEndpointsProtocol + ";" + "AccountName=" + accountName + ";" + "AccountKey=" + accountKey + ";";
      val storageAccount = CloudStorageAccount.parse(storageConnectionString)
      blobClient = storageAccount.createCloudBlobClient();
      container = blobClient.getContainerReference(containerName)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def downloadFile(remote: String): InputStream = {
    try {
      val blob = container.getBlockBlobReference(remote)
      blob.getProperties.getLength
      blob.openInputStream()
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def uploadFile(remote: String, file: File): Unit = {
    try {
      val blob = container.getBlockBlobReference(remote)
      val fileStream: InputStream = new FileInputStream(file)
      val blobOutputStream = blob.openOutputStream()
      val next = -1
      while (fileStream.read != -1)
        blobOutputStream.write(next)
      blobOutputStream.close()
      fileStream.close()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }


  def uploadFile(remote: String, size: Long, fileStream: InputStream): Unit = {
    try {
      val blob = container.getBlockBlobReference(remote)
      blob.upload(fileStream, size)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def getSize(remote: String): Option[Long] = {
    Try {
      val blob = container.getBlockBlobReference(remote)
      blob.getProperties.getLength
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }
}
