package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.connectors.TargetFSConnector
import org.apache.spark.sql.SaveMode

class JDBCtoFS(sourceFormat: String, destFormat: String) extends TargetFSConnector {

  override def source(tableName: String, params: Map[String, String]): JDBCtoFS = {
    logger.info("Reading source data from table :" + tableName)
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

  override def targetFS(destinationFilePath: String, tableName: String, saveMode: SaveMode): Unit = {
    if (destinationFilePath.isEmpty) {
      logger.info("No file path specified,saving data to table with default file path:" + tableName)
      this.df.write.format(destFormat).mode(saveMode).saveAsTable(tableName)
    } else {
      logger.info("Writing data to target file: " + destinationFilePath)
      this.df.write.format(destFormat).mode(saveMode).option("path", destinationFilePath).saveAsTable(tableName)
      logger.info("Saving data to table:" + tableName)
    }
    logger.info("Target Data in table:" + tableName)
    val targetDF = sparkSession.sql("select * from " + tableName)
    targetDF.show(10, false)
  }

  def target(destinationFilePath: String, saveMode: SaveMode): Unit = {
    logger.info("Writing data to target file: " + destinationFilePath)
    this.df.write.format(destFormat).mode(saveMode).save(destinationFilePath)
  }
}

