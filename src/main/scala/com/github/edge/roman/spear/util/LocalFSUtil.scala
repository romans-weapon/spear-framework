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
