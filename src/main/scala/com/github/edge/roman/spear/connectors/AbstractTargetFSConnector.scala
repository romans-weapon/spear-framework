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

abstract class AbstractTargetFSConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String, destFormat: String) with Connector {

  override def targetFS(destinationFilePath: String, tableName: String, saveMode: SaveMode): Unit = {
    if (destinationFilePath.isEmpty) {
      this.df.write.format(destFormat).mode(saveMode).saveAsTable(tableName)
    } else {
      this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(tableName)
    }
    logger.info(s"Write data to target path: ${destinationFilePath} with format: ${sourceFormat} and saved as table ${tableName} completed with status:${SpearCommons.SuccessStatus}")
    show()
  }

  override def targetFS(destinationPath: String, saveMode: SaveMode): Unit = {
    if (destinationPath.isEmpty) {
      throw new Exception("Empty file path specified:" + destinationPath)
    } else {
      this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationPath).save()
      logger.info(s"Write data to target path: ${destinationPath} with format: ${destFormat} completed with status:${SpearCommons.SuccessStatus}")
    }
  }

  def targetFS(destinationPath: String, params: Map[String, String]): Unit= ???

  override def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetJDBC not compatible for given targetType FS")
}
