/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.edge.roman.spear.connectors

import com.github.edge.roman.spear.{Connector, SpearConnector}
import com.github.edge.roman.spear.commons.SpearCommons
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

private [spear] abstract class AbstractMultiTargetConnector(sourceFormat: String) extends AbstractConnector(sourceFormat: String) {
  var dfTransformed: DataFrame = SpearConnector.spark.emptyDataFrame
  var is_transformed: Boolean = false

  override def targetFS(destinationFilePath: String, destFormat: String, saveAsTable: String, params: Map[String, String], saveMode: SaveMode): Unit = {

    if (destinationFilePath.isEmpty) {
      if (saveAsTable.isEmpty) {
        throw new Exception("Neither file_path nor table_name is provided for landing data to destination")
      } else {
        if (is_transformed) {
          dfTransformed.write.format(destFormat).mode(saveMode).saveAsTable(saveAsTable)
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.format(destFormat).mode(saveMode).saveAsTable(saveAsTable)
        }
        logger.info(s"Write data to default path with format:${sourceFormat} and saved as table:${saveAsTable} completed with status:${SpearCommons.SuccessStatus}")
      }
    } else {
      if (saveAsTable.isEmpty) {
        if (is_transformed) {
          dfTransformed.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
        }
        logger.info(s"Write data to target path:${destinationFilePath} with format:${destFormat} completed with status:${SpearCommons.SuccessStatus}")
      } else {
        if (is_transformed) {
          dfTransformed.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
        }
        logger.info(s"Write data to target path:${destinationFilePath} with format:${destFormat} and is saved as table:${saveAsTable} completed with status:${SpearCommons.SuccessStatus}")
      }
    }
  }

  override def targetJDBC(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "soql" =>
        if (is_transformed) {
          dfTransformed.write.format(SpearCommons.SalesforceFormat)
            .options(params)
            .option("sfObject", tableName).save()
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.format(SpearCommons.SalesforceFormat)
            .options(params)
            .option("sfObject", tableName).save()
        }
      case "saql" =>
        if (is_transformed) {
          dfTransformed.write.format(SpearCommons.SalesforceFormat)
            .options(params)
            .option("datasetName", tableName).save()
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.format(SpearCommons.SalesforceFormat)
            .options(params)
            .option("datasetName", tableName).save()
        }
      case _ =>
        val props = new Properties()
        params.foreach { case (key, value) => props.setProperty(key, value) }
        if (is_transformed) {
          dfTransformed.write.mode(saveMode).jdbc(params.getOrElse("url", throw new NullPointerException("No key 'url' found in the target properties!!")), tableName, props)
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write.mode(saveMode).jdbc(props.get("url").toString, tableName, props)
        }
    }
    logger.info(s"Write data to table/object:${tableName} completed with status:${SpearCommons.SuccessStatus} ")
  }

  override def targetNoSQL(objectName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "mongo" =>
        if (is_transformed) {
          val writeConfig = WriteConfig(
            Map("uri" -> params.getOrElse("uri", throw new NullPointerException("No key 'uri' found in the target properties!!")).concat(s"/${objectName}")))
          MongoSpark.save(dfTransformed.write.format("mongo").options(params).mode(saveMode), writeConfig)
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          val writeConfig = WriteConfig(
            Map("uri" -> params.getOrElse("uri", throw new NullPointerException("No key 'uri' found in the target properties!!")).toString.concat(s"/${objectName}")))
          MongoSpark.save(this.df.write.format("mongo").options(params).mode(saveMode), writeConfig)
        }
      case "cassandra" =>
        if (is_transformed) {
          val destdetailsArr = objectName.split("\\.")
          val keySpace = destdetailsArr(0)
          val tableName = destdetailsArr(1)
          dfTransformed.write.format("org.apache.spark.sql.cassandra")
            .options(Map("keyspace" -> keySpace, "table" -> tableName) ++ params)
            .mode(saveMode)
            .save()
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          val destdetailsArr = objectName.split("\\.")
          val keySpace = destdetailsArr(0)
          val tableName = destdetailsArr(1)
          this.df.write.format("org.apache.spark.sql.cassandra")
            .options(Map("keyspace" -> keySpace, "table" -> tableName) ++ params)
            .mode(saveMode)
            .save()
        }
    }
    logger.info(s"Write data to object:${objectName} completed with status:${SpearCommons.SuccessStatus} ")
  }

  override def targetGraphDB(objectName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    destFormat match {
      case "neo4j" =>
        if (is_transformed) {
          dfTransformed.write
            .format("org.neo4j.spark.DataSource")
            .options(params)
            .save()
          is_transformed = false
          dfTransformed = SpearConnector.spark.emptyDataFrame
        } else {
          this.df.write
            .format("org.neo4j.spark.DataSource")
            .options(params)
            .save()
        }
      case _ =>
        throw new Exception("Given destination format for type graph is not supported by spear!!")
    }
    logger.info(s"Write data to object:${objectName} completed with status:${SpearCommons.SuccessStatus} ")
  }

  override def transformSql(sqlText: String): Connector = {
    if (dfTransformed.count() > 0) {
      dfTransformed = dfTransformed.sqlContext.sql(sqlText)
    } else {
      dfTransformed = this.df.sqlContext.sql(sqlText)
    }
    logger.info(s"Executing transformation sql: ${sqlText} status :${SpearCommons.SuccessStatus}")
    is_transformed = true
    dfTransformed.show(10, false)
    this
  }
}
