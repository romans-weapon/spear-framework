package com.github.edge.roman.spear.connectors

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.connectors.commons.SpearCommons
import org.apache.spark.sql.SaveMode

import java.util.Properties


abstract class  AbstractTargetJDBCConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String, destFormat: String) with Connector {

  override def targetJDBC(tableName: String, props: Properties, saveMode: SaveMode): Unit = {
    destFormat match {
      case "soql" =>
        this.df.write.format(SpearCommons.SalesforceFormat)
          .option(SpearCommons.Username, props.get(SpearCommons.Username).toString)
          .option(SpearCommons.Password, props.get(SpearCommons.Password).toString)
          .option("sfObject", tableName).save()
      case "saql" =>
        this.df.write.format(SpearCommons.SalesforceFormat)
          .option(SpearCommons.Username, props.get(SpearCommons.Username).toString)
          .option(SpearCommons.Password, props.get(SpearCommons.Password).toString)
          .option("datasetName", tableName).save()
      case _ =>
        this.df.write.mode(saveMode).jdbc(props.get("url").toString, tableName, props)
    }
    logger.info(s"Write data to table/object ${tableName} completed with status:${SpearCommons.SuccessStatus} ")
    show()
  }

  override def targetFS(destinationFilePath: String, saveAsTable: String, saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for given targetType 'relational'")

  override def targetFS(destinationFilePath: String, saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetFS() not supported for given targetType 'relational'")

  override def targetFS(destinationFilePath: String,params: Map[String, String]): Unit = throw new NoSuchMethodException("method targetFS() not compatible for given targetType 'relational'")
}
