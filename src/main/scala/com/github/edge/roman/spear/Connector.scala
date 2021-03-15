package com.github.edge.roman.spear

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

trait Connector {
  def init(master: String, appName: String):Connector

  def source(source:String):Connector

  def source(source:String,params: Map[String, String]):Connector

  def target(target: String, props: Properties, saveMode: SaveMode):Unit

  def transformSql(sqlText: String):Connector

  def saveAs(alias: String): Connector

  def toDF(): DataFrame

  def cacheData(): Connector

  def mapDataType(string: String):Connector

  def stop()
}
