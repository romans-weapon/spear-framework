package com.github.edge.roman.spear.connectors.commons

import com.databricks.spark.xml.XmlDataFrameReader
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object ConnectorCommon {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def sourceFile(sourceFormat: String, sourceFilePath: String, params: Map[String, String]): DataFrame = {
    sourceFormat match {
      case "csv" =>
        SpearConnector.spark.read.options(params).csv(sourceFilePath)
      case "avro" | "parquet" =>
        SpearConnector.spark.read.format(sourceFormat).options(params).load(sourceFilePath)
      case "json" =>
        SpearConnector.spark.read.options(params).json(sourceFilePath)
      case "tsv" =>
        val _params = params + ("sep" -> "\t")
        SpearConnector.spark.read.options(_params).csv(sourceFilePath)
      case "xml" =>
        SpearConnector.spark.read.format("com.databricks.spark.xml").options(params).xml(sourceFilePath)
      case _ =>
        throw new Exception("Invalid source format provided.")
    }
  }

  def sourceJDBC(sourceObject: String, sourceFormat: String, params: Map[String, String]): DataFrame = {
    sourceFormat match {
      case "soql" | "saql" =>
        throw new NoSuchMethodException(s"Salesforce object ${sourceObject} cannot be loaded directly.Instead use sourceSql function with soql/saql query to load the data")
      case _ =>
        SpearConnector.spark.read.format(sourceFormat).option("dbtable", sourceObject).options(params).load()
    }
  }

  def sourceSQL(sqlText: String, sourceFormat: String, params: Map[String, String]): DataFrame = {
    sourceFormat match {
      case "soql" | "saql" =>
        if (sourceFormat.equals("soql")) {
          SpearConnector.spark.read.format(SpearCommons.SalesforceFormat).option("soql", s"$sqlText").options(params).load()
        } else {
          SpearConnector.spark.read.format(SpearCommons.SalesforceFormat).option("saql", s"$sqlText").options(params).load()
        }
      case _ =>
        SpearConnector.spark.read.format(sourceFormat).option("dbtable", s"($sqlText)temp").options(params).load()
    }
  }
}
