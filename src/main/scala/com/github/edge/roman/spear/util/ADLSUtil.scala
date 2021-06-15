/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.edge.roman.spear.util

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
