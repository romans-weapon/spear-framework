package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.TargetFSConnector
import org.apache.spark.sql.SaveMode

import java.util.Properties

class FiletoFS(sourceType: String,destFormat:String) extends TargetFSConnector {
  override def source(source: String): Connector = ???

  override def source(source: String, params: Map[String, String]): Connector = ???

  override def target(target: String, props: Properties, saveMode: SaveMode): Unit = ???

  override def transformSql(sqlText: String): Connector = ???

  override def target(target: String, objectName: String, saveMode: SaveMode): Unit = ???
}
