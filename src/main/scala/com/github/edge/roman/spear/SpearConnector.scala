package com.github.edge.roman.spear

import com.github.edge.roman.spear.connectors.targetFS.{FiletoFS, JDBCtoFS}
import com.github.edge.roman.spear.connectors.targetjdbc.{FiletoJDBC, JDBCtoJDBC}

class SpearConnector {
  private var sourceType: String = null
  private var sourceFormat: String = null
  private var destType: String = null
  private var destFormat: String = null

  def source(sourceType: String, sourceFormat: String): SpearConnector = {
    this.sourceType = sourceType
    this.sourceFormat = sourceFormat
    this
  }

  def target(targetType: String, targetFormat: String): SpearConnector = {
    this.destType = targetType
    this.destFormat = targetFormat
    this
  }

  def getConnector: Connector = {
    (sourceType, destType) match {
      case ("file", "jdbc") => new FiletoJDBC(sourceFormat)
      case ("jdbc", "jdbc") => new JDBCtoJDBC(sourceFormat)
      case ("file", "FS") => new FiletoFS(sourceFormat)
      case ("jdbc", "FS") => new JDBCtoFS(sourceFormat)
    }
  }
}
