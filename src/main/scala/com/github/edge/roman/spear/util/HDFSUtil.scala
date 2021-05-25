package com.github.edge.roman.spear.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.IOUtils

import java.io.{File, FileInputStream, InputStream}

class HDFSUtil {
  var fileSystem: FileSystem = _
  var bucket_name: String = _

  def configureClient(configMap: Map[String, String]):Unit = {
    try {
      bucket_name = configMap("bucketName")
      val coreSite: String = configMap("core-site")
      val hdfsSite: String = configMap("hdfs-site")
      val conf: Configuration = new Configuration()
      conf.addResource(new Path(coreSite))
      conf.addResource(new Path(hdfsSite))
      conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
      conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
      fileSystem = FileSystem.get(conf)
    } catch {
      case exception: Exception =>exception.printStackTrace()
    }
  }

  def downloadFile(remote: String): InputStream = {
    try {
      val path = new Path(remote)
      val exists = fileSystem.exists(path)
      var inputStream: InputStream = null
      if (exists)
        inputStream=fileSystem.open(path)
      inputStream
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def uploadFile(remote: String, file: File):Unit = {
    try {
      val path = new Path(remote)
      val outPutStream = fileSystem.create(path)
      val inputStream = new FileInputStream(file)
      val conf = new Configuration()
      IOUtils.copyBytes(inputStream, outPutStream, conf, true)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }
  
  def uploadFile(remote: String, size: Long, fileStream: InputStream):Unit = {
    try {
      val path = new Path(remote)
      val outPutStream = fileSystem.create(path)
      val conf = new Configuration()
      IOUtils.copyBytes(fileStream, outPutStream, conf, true)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }

  def getSize(remote: String): Long = {
    var size: Long = 0L
    try {
      size = fileSystem.getFileStatus(new Path(remote)).getLen
    } catch {
      case exception: Exception =>exception.printStackTrace()
    }
    size
  }
}
