package com.github.edge.roman.spear

import com.github.edge.roman.spear.connectors.targetjdbc.{FiletoJDBC, JDBCtoJDBC}

class SpearConnector {
  private var sourceType: String = null
  private var destType: String = null

  def sourceType(sourceType: String): SpearConnector = {
    this.sourceType = sourceType
    this
  }

  def targetType(targetType: String): SpearConnector = {
    this.destType = targetType
    this
  }

  def getConnector: Connector = {
    (sourceType, destType) match {
      case ("csv", "jdbc") => new FiletoJDBC("csv", "jdbc")
      case ("tsv", "jdbc") => new FiletoJDBC("tsv", "jdbc")
      case ("xml", "jdbc") => new FiletoJDBC("xml", "jdbc")
      case ("json", "jdbc") => new FiletoJDBC("json", "jdbc")
      case ("avro", "jdbc") => new FiletoJDBC("avro", "jdbc")
      case ("parquet", "jdbc") => new FiletoJDBC("parquet", "jdbc")
      case ("jdbc", "jdbc") => new JDBCtoJDBC("jdbc", "jdbc")
    }
  }
}
