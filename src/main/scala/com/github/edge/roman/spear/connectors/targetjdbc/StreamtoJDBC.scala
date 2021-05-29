package com.github.edge.roman.spear.connectors.targetjdbc

import com.github.edge.roman.spear.commons.ConnectorCommon
import com.github.edge.roman.spear.connectors.AbstractTargetJDBCConnector
import com.github.edge.roman.spear.{Connector, SpearConnector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

class StreamtoJDBC(sourceFormat: String, destFormat: String) extends AbstractTargetJDBCConnector(sourceFormat, destFormat) {

  override def source(sourceObject: String, params: Map[String, String], schema: StructType): Connector = {
    logger.info(s"Real time streaming Connector to Target: File System with Format: ${destFormat} from Source object: ${sourceObject} with Format: ${sourceFormat} started running!!")
    if (schema.isEmpty) {
      throw new Exception("schema is necessary while straming in real time")
    } else {
      ConnectorCommon.sourceStream(sourceObject, sourceFormat, params, schema)
      this
    }
  }

  override def source(sourceObject: String, params: Map[String, String]): Connector = {
    val _df = SpearConnector.spark
      .readStream
      .format(sourceFormat)
      .options(params)
      .load(sourceObject)
    this.df = _df
    this
  }


  override def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit = {
    this.df.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .mode(saveMode)
          .jdbc(props.get("url").toString, tableName, props)
      }.start()
      .awaitTermination()
  }

  override def targetSql(sqlText: String, props: Properties, saveMode: SaveMode): Unit = {
    this.df.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.sqlContext.sql(sqlText)
      }.start()
      .awaitTermination()
  }
}
