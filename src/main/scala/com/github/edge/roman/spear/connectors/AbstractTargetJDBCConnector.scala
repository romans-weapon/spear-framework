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
import org.apache.spark.sql.SaveMode

import java.util.Properties

abstract class AbstractTargetJDBCConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String) with Connector {

  override def targetJDBC(tableName: String, destFormat: String = destFormat, params: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "soql" =>
        this.df.write.format(SpearCommons.SalesforceFormat)
          .options(params)
          .option("sfObject", tableName).save()
      case "saql" =>
        this.df.write.format(SpearCommons.SalesforceFormat)
          .options(params)
          .option("datasetName", tableName).save()
      case _ =>
        val props = new Properties()
        params.foreach { case (key, value) => props.setProperty(key, value) }
        this.df.write.mode(saveMode).jdbc(params.getOrElse("url",throw new NullPointerException("No key 'url' found in the target properties!!")).toString, tableName, props)
    }
    logger.info(s"Write data to table/object:${tableName} completed with status:${SpearCommons.SuccessStatus} ")
    show()
  }

  //un-supported here
  override def targetFS(destinationFilePath: String, destFormat: String, saveAsTable: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for given targetType 'relational' ")

  override def targetFS(insertIntoTable: String,destFormat: String ,params: Map[String, String], saveMode: SaveMode): Unit= throw new NoSuchMethodException("method targetFS() not supported for given targetType 'relational'")

  override def targetNoSQL(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetNoSQL() not compatible for given targetType 'relational' ")

  override def targetGraphDB(objectName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetGraphDB() not compatible for given targetType 'relational' ")
}
