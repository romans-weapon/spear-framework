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

package com.github.edge.roman.spear.connectors

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.commons.SpearCommons
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SaveMode


abstract class AbstractTargetNoSQLConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String) with Connector {

  override def targetNoSQL(objectName: String, destFormat: String = destFormat, props: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "mongo" =>
        val writeConfig = WriteConfig(
          Map("uri" -> props.getOrElse("uri", throw new NullPointerException("No key 'uri' found in the target properties!!")).concat(s"/${objectName}")))
        MongoSpark.save(this.df.write.format("mongo").options(props).mode(saveMode), writeConfig)
      case "cassandra" =>
        val destdetailsArr = objectName.split("\\.")
        val keySpace = destdetailsArr(0)
        val tableName = destdetailsArr(1)
        this.df.write.format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> keySpace, "table" -> tableName) ++ props)
          .mode(saveMode)
          .save()
    }
    logger.info(s"Write data to destination:${destFormat} object: ${objectName} completed with status:${SpearCommons.SuccessStatus} ")
    show()
  }

  //unsupported here
  override def targetFS(destinationFilePath: String, destFormat: String, saveAsTable: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for given targetType 'nosql' ")

  def targetFS(insertIntoTable: String, destFormat: String, props: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for given targetType 'nosql' ")

  override def targetJDBC(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetJDBC() not compatible for given targetType 'nosql' ")

  override def targetGraphDB(objectName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetGraphDB() not compatible for given targetType 'nosql' ")
}
