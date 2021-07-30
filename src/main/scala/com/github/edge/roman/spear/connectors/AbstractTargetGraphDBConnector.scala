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

abstract class AbstractTargetGraphDBConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String) with Connector {

  override def targetGraphDB(objectName: String, destFormat: String = destFormat, params: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "neo4j" =>
        this.df.write
          .format("org.neo4j.spark.DataSource")
          .options(params)
          .option("label",objectName)
          .save()
      case _ =>
        throw new Exception("Given destination format for type graph is not supported by spear!!")
    }
    logger.info(s"Write data to object:${objectName} completed with status:${SpearCommons.SuccessStatus} ")
    show()
  }

  //unsupported
  override def targetJDBC(tableName: String, destFormat: String = destFormat, params: Map[String, String], saveMode: SaveMode): Unit = throw new Exception("method targetJDBC() is not applicable for dest type graph ")

  override def targetFS(destinationFilePath: String, destFormat: String, saveAsTable: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for dest type graph")

  override def targetFS(insertIntoTable: String,destFormat: String ,params: Map[String, String], saveMode: SaveMode): Unit= throw new NoSuchMethodException("method targetFS() not supported for dest type graph")

  override def targetNoSQL(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetNoSQL() not compatible for dest type graph")

}
