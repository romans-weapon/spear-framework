/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.edge.roman.spear.connectors.targetjdbc

import com.github.edge.roman.spear.commons.ConnectorCommon
import com.github.edge.roman.spear.connectors.AbstractTargetJDBCConnector
import com.github.edge.roman.spear.{Connector, SpearConnector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties

class StreamtoJDBC(sourceFormat: String, destFormat: String) extends AbstractTargetJDBCConnector(sourceFormat, destFormat) {

  override def source(sourceObject: String, params: Map[String, String], schema: StructType): Connector = {
    logger.info(s"Real time streaming Connector to Target: JDBC with Format: ${destFormat} from Source object: ${sourceObject} with Format: ${sourceFormat} started running!!")
    if (schema.isEmpty) {
      throw new Exception("schema is necessary while streaming in real time")
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


  override def targetJDBC(tableName: String,destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    val props = new Properties()
    params.foreach { case (key, value) => props.setProperty(key, value.toString) }
    this.df.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.write
          .mode(saveMode)
          .jdbc(params.get("url").toString, tableName, props)
      }.start()
      .awaitTermination()
  }

  override def targetSql(sqlText: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    this.df.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.sqlContext.sql(sqlText)
      }.start()
      .awaitTermination()
  }
}
