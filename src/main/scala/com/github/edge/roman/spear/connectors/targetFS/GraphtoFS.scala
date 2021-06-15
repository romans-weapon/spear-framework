package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.connectors.AbstractTargetFSConnector

class GraphtoFS(sourceFormat: String, destFormat: String) extends AbstractTargetFSConnector(sourceFormat, destFormat) {

  override def source(labelName: String, params: Map[String, String]): GraphtoFS = {
    logger.info(s"Connector to Target: File System with Format: ${destFormat} from Graph source object: ${labelName} with Format: ${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceGraphDB(labelName, sourceFormat, params)
    logger.info(s"Reading source table: ${labelName} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}
