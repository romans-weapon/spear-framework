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

import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.connectors.AbstractTargetJDBCConnector

class GraphtoJDBC(sourceFormat: String, destFormat: String) extends AbstractTargetJDBCConnector(sourceFormat, destFormat) {

  override def source(labelName: String, params: Map[String, String]): GraphtoJDBC = {
    logger.info(s"Connector:${appName} to Target:JDBC with Format:${destFormat} from GraphDB Object:${labelName} with Format:${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceGraphDB(labelName, sourceFormat, params)
    logger.info(s"Reading source table:${labelName} with format:${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}
