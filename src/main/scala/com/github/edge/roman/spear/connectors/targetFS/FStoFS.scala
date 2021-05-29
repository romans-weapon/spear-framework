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

package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.commons.SpearCommons
import com.github.edge.roman.spear.connectors.AbstractTargetFSConnector
import com.github.edge.roman.spear.util._
import org.apache.spark.sql.SaveMode

import java.io.InputStream

class FStoFS(sourceFormat: String, destFormat: String) extends AbstractTargetFSConnector(sourceFormat, destFormat) {
  private val ftpUtil: FTPUtil = new FTPUtil
  private val s3Util: S3Util = new S3Util
  private val smbUtil: SMBUtil = new SMBUtil
  private val gcsUtil: GCSUtil = new GCSUtil
  private val adlsUtil: ADLSUtil = new ADLSUtil
  private val hdfsUtil: HDFSUtil = new HDFSUtil
  private val localFSUtil: LocalFSUtil = new LocalFSUtil

  private var inputStream: InputStream = _
  private var size: Long = _

  override def source(sourceObject: String, params: Map[String, String]): FStoFS = {
    sourceFormat match {
      case SpearCommons.FTP =>
        ftpUtil.configureClient(params)
        logger.info("FTP Client configured successfully")
        inputStream = ftpUtil.downloadFile(sourceObject).orNull
        size = ftpUtil.getSize(sourceObject).getOrElse(0L)
      case SpearCommons.AWS =>
        s3Util.configureClient(params)
        logger.info("Amazon S3 Client configured successfully")
        inputStream = s3Util.downloadFile(sourceObject)
        size = s3Util.getSize(sourceObject).getOrElse(0L)
      case SpearCommons.SMB =>
        smbUtil.configureClient(params)
        logger.info("SMB Client configured successfully")
        inputStream = smbUtil.downloadFile(sourceObject)
        size = smbUtil.getSize(sourceObject).getOrElse(0L)
      case SpearCommons.GCS =>
        gcsUtil.configureClient(params)
        logger.info("Google Cloud Storage configured successfully")
        inputStream = gcsUtil.downloadFile(sourceObject)
      case SpearCommons.ADLS =>
        adlsUtil.configureClient(params)
        logger.info("Azure Blob Storage configured successfully")
        inputStream = adlsUtil.downloadFile(sourceObject)
      case SpearCommons.HDFS =>
        hdfsUtil.configureClient(params)
        logger.info("Hadoop File System configured successfully")
        inputStream = hdfsUtil.downloadFile(sourceObject)
      case SpearCommons.LOCAL =>
        logger.info("Local File System configured successfully")
        inputStream = localFSUtil.downloadFile(sourceObject)
      case _ =>
        throw new Exception("Invalid source type provided or Not Supported.")
    }
    this
  }

  override def targetFS(destinationPath: String, params: Map[String, String]): Unit = {
    destFormat match {
      case SpearCommons.LOCAL =>
        localFSUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.AWS =>
        s3Util.configureClient(params)
        s3Util.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.GCS =>
        gcsUtil.configureClient(params)
        gcsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.ADLS =>
        adlsUtil.configureClient(params)
        adlsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.HDFS =>
        hdfsUtil.configureClient(params)
        hdfsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case _ =>
        throw new Exception("Invalid destination type provided or Not Supported.")
    }
  }

  override def targetFS(destinationPath: String, saveMode: SaveMode): Unit = {
    destFormat match {
      case SpearCommons.LOCAL =>
        localFSUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.AWS =>
        s3Util.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.GCS =>
        gcsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.ADLS =>
        adlsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case SpearCommons.HDFS =>
        hdfsUtil.uploadFile(destinationPath, size, inputStream)
        logger.info(SpearCommons.FileUploadSuccess)
      case _ =>
        throw new Exception("Invalid destination type provided or Not Supported...")
    }
  }
}
