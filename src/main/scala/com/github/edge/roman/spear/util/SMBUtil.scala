package com.github.edge.roman.spear.util

import com.hierynomus.msdtyp.AccessMask
import com.hierynomus.mssmb2.{SMB2CreateDisposition, SMB2ShareAccess}
import com.hierynomus.smbj.SMBClient
import com.hierynomus.smbj.auth.AuthenticationContext
import com.hierynomus.smbj.connection.Connection
import com.hierynomus.smbj.session.Session
import com.hierynomus.smbj.share.DiskShare

import java.io.InputStream
import java.util
import scala.util.{Failure, Success, Try}

class SMBUtil {

  var diskShare: DiskShare = _

  def configureClient(configMap: Map[String, String]): Try[Unit] = Try {
    try {
      val host: String = configMap("host")
      val domain: String = configMap("domain")
      val share: String = configMap("share")
      val user: String = configMap("user")
      val password: String = configMap("password")
      val client = new SMBClient()
      val smbCon: Connection = client.connect(host)
      val ac = new AuthenticationContext(user, password.toCharArray, domain)
      val session: Session = smbCon.authenticate(ac)
      diskShare = session.connectShare(share).asInstanceOf[DiskShare]
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def downloadFile(remote: String): InputStream = {
    try {
      val file: com.hierynomus.smbj.share.File = diskShare
        .openFile(remote, util.EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL,
          SMB2CreateDisposition.FILE_OPEN,
          null)
     file.getInputStream()
    } catch {
      case exception: Exception => throw new Exception(exception)
    }
  }

  def getSize(remote: String): Option[Long] = {
    Try {
      val file: com.hierynomus.smbj.share.File = diskShare
        .openFile(remote, util.EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL,
          SMB2CreateDisposition.FILE_OPEN,
          null)
       file.getFileInformation().getStandardInformation.getEndOfFile
    } match {
      case Success(a) => Some(a)
      case Failure(f) => None
    }
  }
}
