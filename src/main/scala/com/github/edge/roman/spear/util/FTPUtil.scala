package com.github.edge.roman.spear.util
import org.apache.commons.net.ftp._

import java.io.InputStream
import scala.util.{Failure, Success, Try}

class FTPUtil() {

  private val client = new FTPClient

  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
  }

  def connect(host: String): Unit = {
    try {
      client.connect(host)
      client.enterLocalPassiveMode()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }

  }

  def configureClient(configMap: Map[String, String]): Unit = {
    try {
      val host: String = configMap("host")
      val userName: String = configMap.getOrElse("user", "anonymous")
      val password: String = configMap.getOrElse("password", "anonymous")
      client.connect(host)
      client.enterLocalPassiveMode()
      client.login(userName, password)
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }


  def listFiles(dir: Option[String] = None): List[FTPFile] =
    dir.fold(client.listFiles)(client.listFiles).toList


  def downloadFile(remote: String): Option[InputStream] = {
    Try {
      val stream = client.retrieveFileStream(remote)
      client.completePendingCommand()
      stream
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }

  def uploadFile(remote: String, stream: InputStream): Boolean =
    client.storeFile(remote, stream)

  def getSize(remote: String): Option[Long] = {
    Try {
      val ftpFile = client.mlistFile(remote)
      val size = ftpFile.getSize
      size
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }
}