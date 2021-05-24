package com.github.edge.roman.spear.connectors

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.commons.SpearCommons
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
