package com.github.edge.roman.spear.connectors.targetNoSQL

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.connectors.AbstractTargetNoSQLConnector

class NoSQLtoNoSQL(sourceFormat: String, destFormat: String) extends AbstractTargetNoSQLConnector(sourceFormat, destFormat) {
  override def source(sourceObject: String, params: Map[String, String]): Connector = {
    logger.info(s"Connector to Target: NoSQL with Format: ${destFormat} from Source Object: ${sourceObject} with Format: ${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceNOSQL(sourceObject = sourceObject, sourceFormat, params)
    logger.info(s"Reading source object: ${sourceObject} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}
