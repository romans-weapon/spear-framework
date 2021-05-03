package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.TargetFSConnector
import org.apache.spark.sql.SaveMode


class FiletoFS(sourceType: String,destFormat:String) extends TargetFSConnector {

  override def source(source: String, params: Map[String, String]): Connector = ???

  override def transformSql(sqlText: String): Connector = ???

  override def targetFS(target: String, objectName: String, saveMode: SaveMode): Unit = ???

}
