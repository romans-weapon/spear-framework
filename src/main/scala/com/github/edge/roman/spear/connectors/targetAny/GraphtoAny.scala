package com.github.edge.roman.spear.connectors.targetAny

import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.connectors.AbstractMultiTargetConnector


class GraphtoAny(sourceFormat: String) extends AbstractMultiTargetConnector(sourceFormat) {

  override def source(labelName: String, params: Map[String, String]): GraphtoAny = {
    logger.info(s"MultiTarget connector with name:${appName} from label:${labelName} with format:${sourceFormat} started running !!")
    this.df = ConnectorCommon.sourceGraphDB(labelName, sourceFormat, params)
    logger.info(s"Reading source table:${labelName} with format:${sourceFormat} status:${SpearCommons.SuccessStatus}")
    if (this.verboseLogging) this.df.show(this.numRows, false)
    this
  }
}
