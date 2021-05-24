package com.github.edge.roman.spear.connectors.targetjdbc

import com.github.edge.roman.spear.connectors.AbstractTargetJDBCConnector
import com.github.edge.roman.spear.connectors.commons.{ConnectorCommon, SpearCommons}

class JDBCtoJDBC(sourceFormat: String, destFormat: String) extends AbstractTargetJDBCConnector(sourceFormat,destFormat)  {

  override def source(tableName: String, params: Map[String, String]): JDBCtoJDBC = {
    logger.info(s"Connector to Target: File System with Format: ${destFormat} from Source Object: ${tableName} with Format: ${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceJDBC(tableName, sourceFormat, params)
    logger.info(s"Reading source table: ${tableName} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}
