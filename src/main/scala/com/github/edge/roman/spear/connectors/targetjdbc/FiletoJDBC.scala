package com.github.edge.roman.spear.connectors.targetjdbc

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.AbstractTargetJDBCConnector
import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}


class FiletoJDBC(sourceFormat: String, destFormat: String) extends AbstractTargetJDBCConnector(sourceFormat,destFormat) {

  override def source(sourceFilePath: String, params: Map[String, String]): FiletoJDBC = {
    logger.info(s"Connector to Target: JDBC with Format: ${destFormat} from Source: ${sourceFilePath} with Format: ${sourceFilePath} started running !!")
    this.df = ConnectorCommon.sourceFile(sourceFormat, sourceFilePath, params)
    logger.info(s"Reading source file: ${sourceFilePath} with format: ${sourceFormat} status:${SpearCommons.SuccessStatus}")
    show()
    this
  }
  override def sourceSql(params: Map[String, String], sqlText: String): Connector =throw new NoSuchMethodException(s"method sourceSql is not supported for given sourceType file for connector type FiletoJDBC" )
}
