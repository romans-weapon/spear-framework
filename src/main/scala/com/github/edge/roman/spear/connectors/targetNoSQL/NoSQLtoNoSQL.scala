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

package com.github.edge.roman.spear.connectors.targetNoSQL

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.connectors.AbstractTargetNoSQLConnector

class NoSQLtoNoSQL(sourceFormat: String, destFormat: String) extends AbstractTargetNoSQLConnector(sourceFormat, destFormat) {

  override def source(sourceObject: String, params: Map[String, String]): Connector = {
    logger.info(s"Connector:${appName} to Target: NoSQL with Format:${destFormat} from NoSQL Object:${sourceObject} with Format:${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceNOSQL(sourceObject = sourceObject, sourceFormat, params)
    logger.info(s"Reading source object: ${sourceObject} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }

  override def sourceSql(params: Map[String, String], sqlText: String): Connector =throw new NoSuchMethodException(s"method sourceSql is not supported for given sourceType nosql for connector type NoSQLtoNoSQL" )
}
