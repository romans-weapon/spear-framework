package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.TargetFSConnector
import org.apache.spark.sql.SaveMode

import java.util.Properties


class JDBCtoFS(sourceFormat: String) extends TargetFSConnector {

  override def source(tableName: String, params: Map[String, String]): JDBCtoFS = {
    val df = this.sparkSession.read.format(sourceFormat).option("dbtable", tableName).options(params).load()
    this.df = df
    df.show(10, false)
    this
  }

  override def transformSql(sqlText: String): Connector = {
    logger.info("Data after transformation using the SQL : " + sqlText)
    val _df = this.df.sqlContext.sql(sqlText)
    _df.show(10, false)
    this.df = _df
    this
  }


  override def target(filePath: String, props: Properties, saveMode: SaveMode): Unit = {
    logger.info("Writing data to target file: " + filePath)
    this.df.write.format(props.get("destination_file_format").toString).mode(saveMode).saveAsTable(props.get("destination_table_name").toString)
    val targetDF = sparkSession.sql("select * from " + props.get("destination_table_name").toString)
    targetDF.show(10, false)
  }


  override def source(source: String): Connector = ???
}

