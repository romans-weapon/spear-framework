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

import com.github.edge.roman.spear.Connector
import com.github.edge.roman.spear.commons.SpearCommons
import org.apache.spark.sql.SaveMode

import java.util.Properties

abstract class AbstractTargetFSConnector(sourceFormat: String, destFormat: String) extends AbstractConnector(sourceFormat: String) with Connector {

  override def targetFS(destinationFilePath: String, destFormat: String = destFormat, saveAsTable: String,params: Map[String, String] = Map(), saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val numBuckets = Integer.valueOf(params.get("num_buckets").toString)
    val bucket_column = params.get(SpearCommons.PartitionCols).toString.split(",")(0)
    val bucketCols = params.get(SpearCommons.PartitionCols).toString.split(",").tail
    val partition_columns = params.get(SpearCommons.PartitionCols).toString.split(",")
    if (destinationFilePath.isEmpty) {
      if (saveAsTable.isEmpty) {
        throw new Exception("Neither file_path nor table_name is provided for landing data to destination")
      } else {
        //create partitions in the default warehouse path (/user/hive/warehouse) for hive
        if (params.contains(SpearCommons.PartitionCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).mode(saveMode).saveAsTable(saveAsTable)
        } else if (params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).bucketBy(numBuckets, bucket_column, bucketCols: _*).mode(saveMode).saveAsTable(saveAsTable)
        } else if (params.contains(SpearCommons.PartitionCols) && params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).bucketBy(numBuckets, bucket_column, bucketCols:_*).mode(saveMode).saveAsTable(saveAsTable)
        } else {
          this.df.write.format(destFormat).mode(saveMode).saveAsTable(saveAsTable)
        }
        logger.info(s"Write data to default path with format: ${destFormat} and saved as table ${saveAsTable} completed with status:${SpearCommons.SuccessStatus}")
        show()
      }
    } else {
      //only having table as empty but want to write to a file at destination
      if (saveAsTable.isEmpty) {
        if (params.contains(SpearCommons.PartitionCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
        } else if (params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).bucketBy(numBuckets, bucket_column, bucketCols: _*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
        } else if (params.contains(SpearCommons.PartitionCols) && params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).bucketBy(numBuckets, bucket_column, bucketCols:_*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
        } else {
          this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).save()
        }
        logger.info(s"Write data to target path: ${destinationFilePath} with format: ${destFormat} completed with status:${SpearCommons.SuccessStatus}")
      } else {
        //have both data path and also the table name to create
        if (params.contains(SpearCommons.PartitionCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
        } else if (params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).bucketBy(numBuckets, bucket_column, bucketCols: _*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
        } else if (params.contains(SpearCommons.PartitionCols) && params.contains(SpearCommons.BucketCols)) {
          this.df.write.format(destFormat).partitionBy(partition_columns: _*).bucketBy(numBuckets, bucket_column, bucketCols:_*).mode(saveMode).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
        } else {
          this.df.write.format(destFormat).mode(saveMode).option(SpearCommons.Path, destinationFilePath).option(SpearCommons.Path, destinationFilePath).saveAsTable(saveAsTable)
        }
        logger.info(s"Write data to target path: ${destinationFilePath} with format: ${destFormat} and saved as table ${saveAsTable} completed with status:${SpearCommons.SuccessStatus}")
        show()
      }
    }
  }

  //unsupported here
  override def targetJDBC(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetJDBC() not compatible for given targetType FS")

  override def targetNoSQL(tableName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit = throw new NoSuchMethodException("method targetNoSQL() not compatible for given targetType FS")

  override def targetGraphDB(objectName: String, destFormat: String, params: Map[String, String], saveMode: SaveMode): Unit=throw new NoSuchMethodException("method targetGraphDB() not compatible for given targetType FS")
}
