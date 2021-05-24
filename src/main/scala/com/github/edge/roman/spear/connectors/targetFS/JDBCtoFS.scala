package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.connectors.AbstractTargetFSConnector
import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}

class JDBCtoFS(sourceFormat: String, destFormat: String) extends AbstractTargetFSConnector(sourceFormat, destFormat) {

  override def source(tableName: String, params: Map[String, String]): JDBCtoFS = {
    logger.info(s"Connector to Target: File System with Format: ${destFormat} from Source Object: ${tableName} with Format: ${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceJDBC(tableName, sourceFormat, params)
    logger.info(s"Reading source table: ${tableName} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}

