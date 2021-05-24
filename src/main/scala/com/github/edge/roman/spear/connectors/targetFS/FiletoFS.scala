package com.github.edge.roman.spear.connectors.targetFS

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.AbstractTargetFSConnector
import com.github.edge.roman.spear.connectors.commons.{ConnectorCommon, SpearCommons}


class FiletoFS(sourceFormat: String, destFormat: String) extends AbstractTargetFSConnector(sourceFormat, destFormat) {

  override def source(sourceFilePath: String, params: Map[String, String]): FiletoFS = {
    logger.info(s"Connector to Target: File System with Format: ${destFormat} from Source: ${sourceFilePath} with Format: ${sourceFilePath} started running !!")
    this.df = ConnectorCommon.sourceFile(sourceFormat, sourceFilePath, params)
    logger.info(s"Reading source file: ${sourceFilePath} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    show()
    this
  }
  override def sourceSql(params: Map[String, String], sqlText: String): Connector =throw new NoSuchMethodException(s"method sourceSql is not supported for given sourceType file for connector type FiletoFS" )
}