package com.github.edge.roman.spear.connectors.targetjdbc

import com.databricks.spark.xml.XmlDataFrameReader
import com.github.edge.roman.spear.connectors.{TargetFSConnector}
import org.apache.spark.sql.SaveMode

import java.util.Properties

class FiletoJDBC(sourceFormat: String,destFormat:String) extends TargetFSConnector {

  override def source(sourcePath: String): FiletoJDBC = {
    sourceFormat match {
      case "csv" =>
        val df = sparkSession.read.csv(sourcePath)
        this.df = df
        logger.info("Data after reading from csv in path : " + sourcePath)
        df.show(10, false)
      case "avro" =>
        val df = this.sparkSession.read.format("avro").load(sourcePath)
        this.df = df
        logger.info("Data after reading from avro file in path : " + sourcePath)
        df.show(10, false)
      case "parquet" =>
        val df = this.sparkSession.read.format("parquet").load(sourcePath)
        this.df = df
        logger.info("Data after reading from parquet file in path : " + sourcePath)
        df.show(10, false)
      case "json" =>
        val df = this.sparkSession.read.json(sourcePath)
        this.df = df
        logger.info("Data after reading from json file in path : " + sourcePath)
        df.show(10, false)
      case "tsv" =>
        val df = this.sparkSession.read.csv(sourcePath)
        this.df = df
        df.show(10, false)
      case "xml" =>
        val df = this.sparkSession.read.format("com.databricks.spark.xml").xml(sourcePath)
        this.df = df
        df.show(10, false)
      case _ =>
        throw new Exception("Invalid source type provided.")
    }
    this
  }

  override def source(sourcePath: String, params: Map[String, String]): FiletoJDBC = {
    sourceFormat match {
      case "csv" =>
        val df = this.sparkSession.read.options(params).csv(sourcePath)
        this.df = df
        logger.info("Data after reading from csv in path : " + sourcePath)
        df.show(10, false)
      case "avro" =>
        val df = this.sparkSession.read.format("avro").options(params).load(sourcePath)
        this.df = df
        logger.info("Data after reading from avro file in path : " + sourcePath)
        df.show(10, false)
      case "parquet" =>
        val df = this.sparkSession.read.format("parquet").options(params).load(sourcePath)
        this.df = df
        logger.info("Data after reading from parquet file in path : " + sourcePath)
        df.show(10, false)
      case "json" =>
        val df = this.sparkSession.read.options(params).json(sourcePath)
        this.df = df
        logger.info("Data after reading from json file in path : " + sourcePath)
        df.show(10, false)
      case "tsv" =>
        val df = this.sparkSession.read.options(params).csv(sourcePath)
        this.df = df
        logger.info("Data after reading from tsv file in path : " + sourcePath)
        df.show(10, false)
      case "xml" =>
        val df = this.sparkSession.read.format("com.databricks.spark.xml").options(params).xml(sourcePath)
        this.df = df
        logger.info("Data after reading from xml file in path : " + sourcePath)
        df.show(10, false)
      case _ =>
        throw new Exception("Invalid source type provided.")
    }
    this
  }

  override def target(tableName: String, props: Properties, saveMode: SaveMode): Unit = {
    logger.info("Writing data to target table: " + tableName)
    this.df.write.mode(saveMode).jdbc(props.get("url").toString, tableName, props)
    showTargetData(tableName: String, props: Properties)
  }

  def showTargetData(tableName: String, props: Properties): Unit = {
    logger.info("Showing data in target table  : " + tableName)
    sparkSession.read.jdbc(props.get("url").toString, tableName, props).show(10, false)
  }

  override def transformSql(sqlText: String): FiletoJDBC = {
    logger.info("Data after transformation using the SQL : " + sqlText)
    this.df = this.df.sqlContext.sql(sqlText)
    this.df.show(10, false)
    this
  }

  override def target(filePath: String, objectName: String, saveMode: SaveMode): Unit = ???
}
