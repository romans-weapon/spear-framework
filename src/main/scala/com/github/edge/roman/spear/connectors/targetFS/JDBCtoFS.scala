package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.TargetFSConnector
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties


class JDBCtoFS(sourceFormat: String, destFormat: String) extends TargetFSConnector {

  override def source(tableName: String, params: Map[String, String]): JDBCtoFS = {
    val df = this.sparkSession.read.format(sourceFormat).option("dbtable", tableName).options(params).load()
    this.df = df
    df.show(10, false)
    this
  }

   override def sourceSql(params: Map[String, String], sqlText: String): JDBCtoFS = {
    logger.info("Executing source sql query: " + sqlText)
    val _df = sparkSession.read.format(sourceFormat).option("dbtable", s"($sqlText)temp").options(params).load()
    this.df = _df
    this
  }

  override def transformSql(sqlText: String): JDBCtoFS = {
    logger.info("Data after transformation using the SQL : " + sqlText)
    val _df = this.df.sqlContext.sql(sqlText)
    _df.show(10, false)
    this.df = _df
    this
  }

  override def target(filePath: String, tableName: String, saveMode: SaveMode): Unit = {
    logger.info("Writing data to target file: " + filePath)
    this.df.write.format(destFormat).mode(saveMode).saveAsTable(tableName);
    val targetDF = sparkSession.sql("select * from " + tableName)
    targetDF.show(10, false)
  }

  def target(filePath: String, saveMode: SaveMode): Unit = {
    logger.info("Writing data to target file: " + filePath)
    this.df.write.format(destFormat).mode(saveMode).save(filePath)
  }


  override def source(source: String): Connector = ???

  override def target(target: String, props: Properties, saveMode: SaveMode): Unit = ???
}

