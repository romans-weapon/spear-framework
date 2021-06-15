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

import org.apache.commons.io.{FileUtils, IOUtils}

import java.io._
import scala.util.{Failure, Success, Try}

class LocalFSUtil {


  def downloadFile(remote: String): InputStream = {
    try {
      new FileInputStream(new File(remote))
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def uploadFile(remote: String, file: File): Unit = {
    try {
      val outputStream: OutputStream = new FileOutputStream(new File(remote))
      val inputStream: InputStream = new FileInputStream(file)
      IOUtils.copy(inputStream, outputStream)
      outputStream.close()
      inputStream.close()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def uploadFile(remote: String, size: Long, fileStream: InputStream): Unit = {
    try {
      val outputStream: OutputStream = new FileOutputStream(new File(remote))
      IOUtils.copy(fileStream, outputStream)
      outputStream.close()
      fileStream.close()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def getSize(remote: String): Option[Long] = {
    Try {
      FileUtils.getFile(remote).length()
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }
}
