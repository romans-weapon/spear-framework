package com.github.edge.roman.spear

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

trait Connector {
  def init(master: String, appName: String):Connector

  def source(sourceObject:String,params: Map[String, String]):Connector

  def sourceSql(params: Map[String, String],sqlText:String):Connector

  def transformSql(sqlText: String):Connector

  def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit

  def targetFS(destinationFilePath: String, saveAsTable: String, saveMode: SaveMode): Unit

  def saveAs(alias: String): Connector

  def toDF: DataFrame

  def cacheData(): Connector

  def stop()
}
