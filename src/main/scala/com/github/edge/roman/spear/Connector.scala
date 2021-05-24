package com.github.edge.roman.spear

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import java.util.Properties

trait Connector {

  def source(sourceObject: String, params: Map[String, String] = Map()): Connector

  def source(sourceObject: String, params: Map[String, String], schema: StructType): Connector

  def sourceSql(params: Map[String, String], sqlText: String): Connector

  def transformSql(sqlText: String): Connector

  def targetFS(destinationFilePath: String, saveAsTable: String, saveMode: SaveMode): Unit

  def targetFS(destinationFilePath: String, saveMode: SaveMode): Unit

  def targetFS(destinationPath: String, params: Map[String, String]): Unit

  def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit

  def targetSql(sqlText: String, props: Properties, saveMode: SaveMode): Unit

  def saveAs(alias: String): Connector

  def cacheData(): Connector

  def repartition(n: Int): Connector

  def coalesce(n: Int): Connector
}

