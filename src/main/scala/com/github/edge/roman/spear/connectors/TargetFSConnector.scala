package com.github.edge.roman.spear.connectors

import com.github.edge.roman.spear.Connector
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


trait TargetFSConnector extends Connector {
  val logger = Logger.getLogger(this.getClass.getName)

  var sparkSession: SparkSession = null
  var df: DataFrame = null

  override def init(master: String, appName: String): TargetFSConnector = {
    sparkSession = SparkSession.builder().master(master).appName(appName).enableHiveSupport().getOrCreate()
    this
  }

  override def saveAs(alias: String): TargetFSConnector = {
    logger.info("Data is saved as a temporary table by name: " + alias)
    logger.info("Showing saved data from temporary table with name: " + alias)
    this.df.createOrReplaceTempView(alias)
    df.show(10, false)
    this
  }

  override def stop(): Unit = {
    this.sparkSession.stop()
  }

  override def toDF: DataFrame = {
    logger.info("Data will now be available as dataframe object")
    this.df
  }

  override def cacheData(): TargetFSConnector = {
    this.df.cache()
    this
  }


  override def targetFS(destinationFilePath: String, saveAsTable: String, saveMode: SaveMode): Unit

  override def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit = ???

  override def sourceSql(params: Map[String, String], sqlText: String): TargetFSConnector = ???
}
