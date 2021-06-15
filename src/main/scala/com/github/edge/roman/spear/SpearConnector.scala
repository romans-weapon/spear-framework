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

import com.github.edge.roman.spear.connectors.AbstractConnector
import com.github.edge.roman.spear.commons.SpearCommons
import com.github.edge.roman.spear.connectors.targetAny.{FiletoAny, GraphtoAny, JDBCtoAny, NOSQLtoAny}
import com.github.edge.roman.spear.connectors.targetFS.{FStoFS, FiletoFS, JDBCtoFS, NOSQLtoFS}
import com.github.edge.roman.spear.connectors.targetGraphDB.{FiletoGraphDB, GraphtoGraphDB, JDBCtoGraphDB, NOSQLtoGraphDB}
import com.github.edge.roman.spear.connectors.targetNoSQL.{FilettoNoSQL, JDBCtoNoSQL, NoSQLtoNoSQL}
import com.github.edge.roman.spear.connectors.targetjdbc.{FiletoJDBC, GraphtoJDBC, JDBCtoJDBC, NOSQLtoJDBC}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SpearConnector {

  lazy val sparkConf = new SparkConf
  lazy val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def createConnector(name: String): SpearConnector = {
    sparkConf.setAppName(name)
    new SpearConnector
  }

  //companion class spear-connector
  class SpearConnector {
    private var sourceType: String = SpearCommons.Star
    private var sourceFormat: String = SpearCommons.Star
    private var destType: String = SpearCommons.Star
    private var destFormat: String = SpearCommons.Star

    def source(sourceType: String, sourceFormat: String): SpearConnector = {
      this.sourceType = sourceType
      this.sourceFormat = sourceFormat
      this
    }

    def target(targetType: String, targetFormat: String): SpearConnector = {
      this.destType = targetType
      this.destFormat = targetFormat
      this
    }

    def multiTarget: SpearConnector = {
      this.destType = SpearCommons.Star
      this
    }

    //connector mapping for source-dest combination which are supported by spear
    def getConnector: AbstractConnector = {
      (sourceType, destType) match {
        //Different sources with JDBC as target
        case (SpearCommons.File, SpearCommons.Relational) => new FiletoJDBC(sourceFormat, destFormat)
        case (SpearCommons.Relational, SpearCommons.Relational) => new JDBCtoJDBC(sourceFormat, destFormat)
        case (SpearCommons.NoSql, SpearCommons.Relational) => new NOSQLtoJDBC(sourceFormat, destFormat)
        case (SpearCommons.Graph, SpearCommons.Relational) => new GraphtoJDBC(sourceFormat, destFormat)

        //Different sources with FS(Hadoop/S3/ADLS/GCS) as target
        case (SpearCommons.File, SpearCommons.FileSystem) => new FiletoFS(sourceFormat, destFormat)
        case (SpearCommons.Relational, SpearCommons.FileSystem) => new JDBCtoFS(sourceFormat, destFormat)
        case (SpearCommons.FileSystem, SpearCommons.FileSystem) => new FStoFS(sourceFormat, destFormat)
        case (SpearCommons.NoSql, SpearCommons.FileSystem) => new NOSQLtoFS(sourceFormat, destFormat)

        //Different sources with Nosql as target
        case (SpearCommons.File, SpearCommons.NoSql) => new FilettoNoSQL(sourceFormat, destFormat)
        case (SpearCommons.Relational, SpearCommons.NoSql) => new JDBCtoNoSQL(sourceFormat, destFormat)
        case (SpearCommons.NoSql, SpearCommons.NoSql) => new NoSQLtoNoSQL(sourceFormat, destFormat)

        //Different sources with GraphDB ass target
        case (SpearCommons.File, SpearCommons.Graph) => new FiletoGraphDB(sourceFormat, destFormat)
        case (SpearCommons.Relational, SpearCommons.Graph) => new JDBCtoGraphDB(sourceFormat, destFormat)
        case (SpearCommons.NoSql, SpearCommons.Graph) => new NOSQLtoGraphDB(sourceFormat, destFormat)
        case (SpearCommons.Graph, SpearCommons.Graph) => new GraphtoGraphDB(sourceFormat, destFormat)

        //Different sources with Any target
        case (SpearCommons.File, SpearCommons.Star) => new FiletoAny(sourceFormat)
        case (SpearCommons.Relational, SpearCommons.Star) => new JDBCtoAny(sourceFormat)
        case (SpearCommons.NoSql, SpearCommons.Star) => new NOSQLtoAny(sourceFormat)
        case (SpearCommons.Graph, SpearCommons.Star) => new GraphtoAny(sourceFormat)

        //throw an exception in case of invalid params
        case (_, _) => throw new Exception(SpearCommons.InvalidParams)
      }
    }
  }
}
