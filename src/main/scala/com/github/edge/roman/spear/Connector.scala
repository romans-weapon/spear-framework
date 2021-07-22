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

package com.github.edge.roman.spear

import com.github.edge.roman.spear.commons.SpearCommons
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

private [spear] trait Connector {

  def source(sourceObject: String, params: Map[String, String] = Map()): Connector

  def source(sourceObject: String, params: Map[String, String] , schema: StructType): Connector

  def sourceSql(params: Map[String, String] = Map(), sqlText: String): Connector

  def transformSql(sqlText: String): Connector

  def executeQuery(sqlText: String): Connector

  def targetFS(destinationFilePath: String=SpearCommons.DefaultFSPath, destFormat: String = SpearCommons.Parquet, saveAsTable: String = "", props: Map[String, String] = Map(), saveMode: SaveMode): Unit

  def targetNoSQL(objectName: String, destFormat: String = SpearCommons.NoSql, props: Map[String, String], saveMode: SaveMode): Unit

  def targetJDBC(objectName: String, destFormat: String = SpearCommons.Jdbc, props: Map[String, String], saveMode: SaveMode): Unit

  def targetGraphDB(objectName: String, destFormat: String = SpearCommons.Graph, props: Map[String, String], saveMode: SaveMode): Unit

  def targetSql(sqlText: String, props: Map[String, String] = Map(), saveMode: SaveMode): Unit

  def targets(targets: Unit*): Unit

  def saveAs(alias: String): Connector

  def createUDF(fucntionName: String, function: UserDefinedFunction): UserDefinedFunction

  def branch: Connector

  def toDF: DataFrame

  def cacheData(): Connector

  def repartition(n: Int): Connector

  def coalesce(n: Int): Connector

  def stop(): Unit
}

