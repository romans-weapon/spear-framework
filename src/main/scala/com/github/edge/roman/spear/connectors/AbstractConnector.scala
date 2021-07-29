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

import com.github.edge.roman.spear.commons.{ConnectorCommon, SpearCommons}
import com.github.edge.roman.spear.{Connector, SpearConnector}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}


private[spear] abstract class AbstractConnector(sourceFormat: String) extends Connector {
  @transient lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

  //variables for spear
  val numRows: Int = SpearCommons.ShowNumRows
  var df: DataFrame = _
  var verboseLogging: Boolean = false
  lazy val appName: String = SpearConnector.sparkConf.get(SpearCommons.AppName)

  def setVeboseLogging(enable: Boolean): Unit = {
    this.verboseLogging = enable
  }

  def saveAs(alias: String): Connector = {
    this.df.createOrReplaceTempView(alias)
    logger.info(s"Saving data as temporary table:${alias} ${SpearCommons.SuccessStatus}")
    this
  }

  def cacheData(): Connector = {
    this.df.cache()
    logger.info(s"Cached data in dataframe: ${SpearCommons.SuccessStatus}")
    this
  }

  def repartition(n: Int): Connector = {
    this.df.repartition(n)
    logger.info(s"Repartition data in dataframe: ${SpearCommons.SuccessStatus}")
    this
  }

  def coalesce(n: Int): Connector = {
    this.df.coalesce(n)
    logger.info(s"Coalesce data in dataframe: ${SpearCommons.SuccessStatus}")
    this
  }

  override def source(sourceObject: String, params: Map[String, String], schema: StructType): Connector = {
    val paramsWithSchema = params + (SpearCommons.CustomSchema -> schema.toString())
    source(sourceObject, paramsWithSchema)
  }

  override def sourceSql(params: Map[String, String], sqlText: String): Connector = {
    logger.info(s"Connector:${appName} with Source sql: ${sqlText} with Format: ${sourceFormat} started running!!")
    this.df = ConnectorCommon.sourceSQL(sqlText, sourceFormat, params)
    logger.info(s"Executing source sql query: ${sqlText} with format: ${sourceFormat} completed with status:${SpearCommons.SuccessStatus}")
    show()
    this
  }

  override def transformSql(sqlText: String): Connector = {
    this.df =  this.df.sqlContext.sql(sqlText)
    logger.info(s"Executing transformation sql: ${sqlText} status :${SpearCommons.SuccessStatus}")
    show()
    this
  }

  override def targetSql(sqlText: String, params: Map[String, String], saveMode: SaveMode): Unit = {
    this.df.sqlContext.sql(sqlText)
  }

  override def executeQuery(sqlText: String): Connector = {
    this.df = SpearConnector.spark.sql(sqlText)
    logger.info(s"Executing spark sql: ${sqlText} status :${SpearCommons.SuccessStatus}")
    show()
    this
  }

  def branch: Connector = {
    this.df.cache()
    logger.info(s"Caching intermediate Dataframe completed with status :${SpearCommons.SuccessStatus}")
    this
  }

  override def targets(targets: Unit*): Unit = {
    /**
     * take multiple targets as params and since all the params are function calls to write data
     * to target these will be executed one after the other form left to right as per the scala rules.
     */

  }

  override def toDF: DataFrame = this.df

  override def stop(): Unit = SpearConnector.spark.stop()

  def show(): Unit = if (this.verboseLogging) this.df.show(this.numRows, false)

}
