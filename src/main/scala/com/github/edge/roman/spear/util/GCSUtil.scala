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
