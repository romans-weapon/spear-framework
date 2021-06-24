# Spear Framework

[![Build Status](https://github.com/romans-weapon/spear-framework/workflows/spear-framework-build/badge.svg)](https://github.com/romans-weapon/spear-framework/actions)
[![Code Quality Grade](https://www.code-inspector.com/project/23492/status/svg)](https://www.code-inspector.com/project/23492/status/svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.romans-weapon/spear-framework_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.github.romans-weapon%22%20AND%20a:%22spear-framework_2.12%22)
[![Website shields.io](https://img.shields.io/website-up-down-green-red/http/shields.io.svg)](https://romans-weapon.github.io/spear-framework/)

The spear-framework provides scope to write simple ETL/ELT-connectors/pipelines for moving data from different sources to different destinations which greatly minimizes the effort of writing complex codes for data ingestion. Connectors which have the ability to extract and load (ETL or ELT) any kind of data from source with custom tansformations applied can be written and executed seamlessly using spear connectors.

# Table of Contents
- [Introduction](#introduction)
- [Design and Code Quality](#design-and-code-quality)
- [Getting started with Spear](#getting-started-with-spear)
    * [SBT dependency for Spear](#sbt-dependency-for-spear)
    * [Maven dependency for Spear](#maven-dependency-for-spear)
    * [Spark shell package for Spear](#spark-shell-package-for-spear)
    * [Docker container setup for Spear](#docker-container-setup-for-spear)
- [Develop your first connector using Spear](#develop-your-first-connector-using-spear)
- [Example Connectors](#example-connectors)
    * [Target JDBC](#target-jdbc)
        - [File Source](#file-source)
            + [CSV to JDBC Connector](#csv-to-jdbc-connector)
        - [JDBC Source](#jdbc-source)
            + [Oracle to Postgres Connector](#oracle-to-postgres-connector)
        - [Streaming Source](#streaming-source)
            + [kafka to Postgres Connector](#kafka-to-postgres-connector)
    * [Target FS (HDFS)](#target-fs-hdfs)
        - [JDBC Source](#jdbc-source)
            + [Postgres to Hive Connector](#postgres-to-hive-connector)
        - [Streaming Source](#streaming-source)
            + [kafka to Hive Connector](#kafka-to-hive-connector)
    * [Target FS (Cloud)](#target-fs-cloud)
        + [Oracle to S3 Connector](#oracle-to-s3-connector)
    * [Target NOSQL](#target-nosql)
        - [File Source](#file-source)
            + [CSV to MongoDB Connector](#csv-to-mongodb-connector)
    * [Target GraphDB](#target-graphdb)
        - [File Source](#file-source)
            + [CSV to neo4j Connector](#csv-to-neo4j-connector)
- [Other Functionalities of Spear](#other-functionalities-of-spear)
    * [Merge using executeQuery API](#merge-using-executequery-api)
    * [Write to multi-targets using branch API](#write-to-multi-targets-using-branch-api)
- [Contributions and License](#contributions-and-license)
- [Visit Website](#visit-website)

# Introduction

Spear Framework provides the developers thae ability to write connectors (ETL/ELT jobs) from a source to a target,applying business logic/transformations over the soure data and ingesting it to the corresponding destination with very minimal code.

![image](https://user-images.githubusercontent.com/59328701/122396653-d412a300-cf95-11eb-8bd5-bef400c07de8.png)

# Design and Code Quality

![image](https://user-images.githubusercontent.com/59328701/122229966-d661f800-ced6-11eb-839a-c77ca7cca610.png)


# Getting Started with Spear
This branch(spark-3.1.1) for the framework has support for **spark-3.x** with **scala 2.12.x** 

You can get started with spear using any of the below methods:

### SBT dependency for Spear

You can add spear-framework as dependency in your projects build.sbt file as show below
```commandline
libraryDependencies += "io.github.romans-weapon" % "spear-framework_2.12" % "3.1.1-1.0"

```

### Maven dependency for Spear
Maven dependency for spear is shown below:
```commandline
<dependency>
  <groupId>io.github.romans-weapon</groupId>
  <artifactId>spear-framework_2.12</artifactId>
  <version>3.1.1-1.0</version>
</dependency>
```

### Spark shell package for Spear

You can also add it as a package while staring spark-shell along with other packages.
```commandline
spark-shell --packages "io.github.romans-weapon:spear-framework_2.12:3.1.1-1.0"
```

### Docker container setup for Spear
Below are the simple steps to setup spear on any machine having docker and docker-compose installed :

1. Clone the repository from git and navigate to project directory
```commandline
git clone https://github.com/AnudeepKonaboina/spear-framework.git && cd spear-framework
```

2. Run setup.sh script using the command
```commandline
sh setup.sh
```

3. Once the setup is completed run the below command for entering into the container containing spear
```commandline
user@node~$ docker exec -it spear bash
```

4. Run `spear-shell` inside the conatiner to start spark shell integrated with spear .
```
root@hadoop # spear-shell
```
5. Once you enter into the conatiner you will get default hadoop/hive environment readily available to read data from any source and write it to HDFS so that it gives you complete environment to create your own data-pipelines using spear-framework.\
Services and their corresponding versions available within the container are shown below:

| Service      | Version     |
| -----------  | ----------- |
| Spark        | 3.1.1       |
| Hadoop       | 3.2.0       |
| Hive         | 3.1.1       |

Also it has a postgres database and a NO-SQL database mongodb as well which you can use it as a source or as a desination for writing and testing your connector.

5. Start writing your own connectors and explore .To understand how to write a connector [click here](develop-your-first-connector-using-spear)




# Develop your first connector using Spear

Below are the steps to write any connector:

1. Get the suitable connector object using Spearconnector by providing the source and destination details as shown below:\

a. For connectors from single source to single destination below is how you create a connector object
```commandline
import com.github.edge.roman.spear.SpearConnector

val connector = SpearConnector
  .createConnector(name="defaultconnector")                //give a name to your connector(any name)
  .source(sourceType = "relational", sourceFormat = "jdbc")//source type and format for loading data  
  .target(targetType = "FS", targetFormat = "parquet")     //target type and format for writing data to dest.
  .getConnector 
```
b. For connectors from a source to multiple targets below is how you create a connector object

```commandline
val multiTargetConnector = SpearConnector
  .createConnector(name="defaultconnector")                //give a name to your connector(any name)
  .source(sourceType = "relational", sourceFormat = "jdbc")//source type and format for loading data  
  .multitarget                                             //use multitarget in case of more than on destinations
  .getConnector 
```

2. Below are the source and destination type combinations that spear-framework supports:

#### Connector types (source and dest.) supported by spear:

|source type  | dest. type    | description                                                | 
|------------ |:-------------:|:-----------------------------------------------------------:
| file        |  relational   |connector object with file source and database as dest.     |
| relational  |  relational   |connector object with database source and database as dest. |
| stream      |  relational   |connector object with stream source and database as dest.   |
| nosql       |  relational   |connector object with nosql  source and relational as dest. |
| graph       |  relational   |connector object with graph source and database as dest.    |
| file        |  FS           |connector object with file source and FileSystem as dest.   |
| relational  |  FS           |connector object with database source and FileSystem as dest|
| stream      |  FS           |connector object with stream source and FileSystem as dest. |
| FS          |  FS           |connector object with FileS  source and FileSystem as dest. |
| graph       |  FS           |connector object with graph  source and FileSystem as dest. |
| nosql       |  FS           |connector object with nosql  source and FileSystem as dest. |
| file        |  nosql        |connector object with nosql  source and nosql      as dest. |
| relational  |  nosql        |connector object with nosql  source and nosql      as dest. |
| nosql       |  nosql        |connector object with nosql  source and nosql      as dest. |
| graph       |  nosql        |connector object with graph  source and nosql      as dest. |
| file        |  graph        |connector object with file  source and  Graph      as dest. |
| relational  |  graph        |connector object with relational  source and Graph as dest. |
| nosql       |  graph        |connector object with nosql  source and Graph      as dest. |



3. Write the connector logic using the connector object in step 1.

```commandline
-> Souce object and connection profile needs to be specified for reading data from source
connector
  .source(sourceObject="<filename/tablename/topic/api>", <source_connection_profile Map((key->value))>) 
  (or) 
  .sourceSql(<connection profile>,<sql_text>)
  
->The saveAs api creates a temporary table on the source data with the given alias name which can be used for further transformations
  .saveAs("<alias temporary table name>")

->apply custom tranformations on the loaded source data.(optional/can be applied only if necessary)
  .transformSql("<transformation sql to be applied on source data>")

->target details where you want to load the data.
  .targetFS(destinationFilePath = "<hdfs /s3/gcs file path>", saveAsTable = "<tablename>", <Savemode can be overwrite/append/ignore>) 
  (or)
  .targetJDBC(tableName=<table_name>, targetProperties, <Savemode can be overwrite/append/ignore>)
  (or)
  .targetNoSQL(<nosql_obj_name>,targetProperties,<Savemode can be overwrite/append/ignore>)
  
 ->for multitarget use the branch api.The dest format will be given whithin the target which will be shown in the examples below.
   .branch
   .targets(
   //target-1
   //target-2
   ..
   //target-n
   )
```

3. On completion stop the connector.

```commandline
//stops the connector object and the underlying spark session
connector.stop()
```

4. Enable verbose logging
   To get the output df at each stage in your connector you can explicitly enable verbose logging as below as soon as you a connector object.This is completely optional.

```commandline
connector.setVeboseLogging(true) //default value is false.
```

## Diagramatic Representation:
![image](https://user-images.githubusercontent.com/59328701/119258939-7afb5d80-bbe9-11eb-837f-02515cb7cf74.png)

## Example Connectors
Connector is basically the logic/code which allows you to create a pipeline from source to target using the spear framework, using which you can write data from any source to any destination.

### Target JDBC
Spear framework supports writing data to any RDBMS with jdbc as destination(postgres/oracle/msql etc..)  from various sources like a file(csv/json/filesystem etc..)/database(RDBMS/cloud db etc..)/streaming(kafka/dir path etc..).Given below are examples of few connectors with JDBC as target.Below examples are written for postgresql as JDBC target,but this can be extended for any jdbc target.

### File source

#### CSV to JDBC Connector
An example connector for reading csv file applying transformations and storing it into postgres table using spear:\

The input data is available in the data/us-election-2012-results-by-county.csv. Simply copy the below connector and paste it in your interactive shell and see your data being moved to a table in postgres with such minimal code !!!.

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)
val targetProps = Map(
    "driver" -> "org.postgresql.Driver",
    "user" -> "postgres_user",
    "password" -> "mysecretpassword",
    "url" -> "jdbc:postgresql://postgres:5432/pgdb"
  )

val csvJdbcConnector = SpearConnector
    .createConnector(name="CSVtoPostgresConnector")
    .source(sourceType = "file", sourceFormat = "csv")
    .target(targetType = "relational", targetFormat = "jdbc")
    .getConnector   
 
csvJdbcConnector.setVeboseLogging(true)
csvJdbcConnector
  .source(sourceObject="file:///opt/spear-framework/data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("__tmp__")
  .transformSql(
    """select state_code,party,
      |sum(votes) as total_votes
      |from __tmp__
      |group by state_code,party""".stripMargin)
  .targetJDBC(objectName="mytable", props=targetProps, saveMode=SaveMode.Overwrite)
csvJdbcConnector.stop()
```

##### Output:

```
21/06/17 08:04:03 INFO targetjdbc.FiletoJDBC: Connector:CSVtoPostgresConnector to Target:JDBC with Format:jdbc from Source:file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with Format:csv started running !!
21/06/17 08:04:09 INFO targetjdbc.FiletoJDBC: Reading source file: file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with format: csv status:success
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|country_id|state_code|country_name|country_total_votes|party|first_name|last_name|votes|
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|1         |AK        |Alasaba     |220596             |Dem  |Barack    |Obama    |91696|
|2         |AK        |Akaskak     |220596             |Dem  |Barack    |Obama    |91696|
|3         |AL        |Autauga     |23909              |Dem  |Barack    |Obama    |6354 |
|4         |AK        |Akaska      |220596             |Dem  |Barack    |Obama    |91696|
|5         |AL        |Baldwin     |84988              |Dem  |Barack    |Obama    |18329|
|6         |AL        |Barbour     |11459              |Dem  |Barack    |Obama    |5873 |
|7         |AL        |Bibb        |8391               |Dem  |Barack    |Obama    |2200 |
|8         |AL        |Blount      |23980              |Dem  |Barack    |Obama    |2961 |
|9         |AL        |Bullock     |5318               |Dem  |Barack    |Obama    |4058 |
|10        |AL        |Butler      |9483               |Dem  |Barack    |Obama    |4367 |
+----------+----------+------------+-------------------+-----+----------+---------+-----+
only showing top 10 rows

21/06/17 08:04:10 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:__tmp__ success
21/06/17 08:04:12 INFO targetjdbc.FiletoJDBC: Executing transformation sql: select state_code,party,
sum(votes) as total_votes
from __tmp__
group by state_code,party status :success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

21/06/17 08:04:17 INFO targetjdbc.FiletoJDBC: Write data to table/object:mytable completed with status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

```
A lot of connectors from other file source to JDBC destination are avaialble [here](https://romans-weapon.github.io/spear-framework/#file-source).


### JDBC source

#### Oracle to Postgres Connector
This example shows the usage of sourceSql api for reading from source with filters applied on the source query as ahown below.

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val targetParams = Map(
  "driver" -> "org.postgresql.Driver",
  "user" -> "postgres_user",
  "password" -> "mysecretpassword",
  "url" -> "jdbc:postgresql://localhost:5432/pgdb"
)

val oracleTOPostgresConnector = SpearConnector
  .createConnector(name="OracletoPostgresConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "relational", targetFormat = "jdbc")
  .getConnector

oracleTOPostgresConnector.setVeboseLogging(true)

oracleTOPostgresConnector
  .sourceSql(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "user" -> "user", "password" -> "pass", "url" -> "jdbc:oracle:thin:@ora-host:1521:orcl"),
    """
      |SELECT
      |        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
      |        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
      |        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
      |        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
      |        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
      |        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
      |        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
      |        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc,
      |        to_char(TIMESTAMP0_WITH_LTZ) as timestamp0_with_ltz , to_char(sys_extract_utc(TIMESTAMP0_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_ltz_utc,
      |        to_char(TIMESTAMP5_WITH_LTZ) as timestamp5_with_ltz , to_char(sys_extract_utc(TIMESTAMP5_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_ltz_utc,
      |        to_char(TIMESTAMP8_WITH_LTZ) as timestamp8_with_ltz , to_char(sys_extract_utc(TIMESTAMP8_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_ltz_utc
      |        from DBSRV.ORACLE_TIMESTAMPS
      |""".stripMargin)
  .saveAs("__source__")
  .transformSql(
    """
      |SELECT
      |        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
      |        TIMESTAMP_0 as timestamp_0,
      |        TIMESTAMP_5 as timestamp_5,
      |        TIMESTAMP_7 as timestamp_7,
      |        TIMESTAMP_9 as timestamp_9,
      |        TIMESTAMP0_WITH_TZ as timestamp0_with_tz,TIMESTAMP0_WITH_TZ_utc as timestamp0_with_tz_utc,
      |        TIMESTAMP5_WITH_TZ as timestamp5_with_tz,TIMESTAMP5_WITH_TZ_utc as timestamp5_with_tz_utc,
      |        TIMESTAMP8_WITH_TZ as timestamp8_with_tz,TIMESTAMP8_WITH_TZ_utc as timestamp8_with_tz_utc,
      |        TIMESTAMP0_WITH_LTZ as timestamp0_with_ltz,TIMESTAMP0_WITH_LTZ_utc as timestamp0_with_ltz_utc,
      |        TIMESTAMP5_WITH_LTZ as timestamp5_with_ltz,TIMESTAMP5_WITH_LTZ_utc as timestamp5_with_ltz_utc,
      |        TIMESTAMP8_WITH_LTZ as timestamp8_with_ltz,TIMESTAMP8_WITH_LTZ_utc as timestamp8_with_ltz_utc
      |        from __source__
      |""".stripMargin)
  .targetJDBC(objectName = "pgdb.ora_to_postgres", params=targetParams, saveMode=SaveMode.Overwrite)

oracleTOPostgresConnector.stop()

```

### Output

```commandline
21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: Executing source sql query:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc
        from DBSRV.ORACLE_TIMESTAMPS

21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: Data is saved as a temporary table by name: __source__
21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: showing saved data from temporary table with name: __source__
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|INGEST_TS_UTC             |TIMESTAMP_0          |TIMESTAMP_5              |TIMESTAMP_7                |TIMESTAMP_9                  |TIMESTAMP0_WITH_TZ                 |TIMESTAMP0_WITH_TZ_UTC|TIMESTAMP5_WITH_TZ                       |TIMESTAMP5_WITH_TZ_UTC   |TIMESTAMP8_WITH_TZ                          |TIMESTAMP8_WITH_TZ_UTC      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-04 17:35:50.620944|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-04 17:35:50.620944|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+

21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: Data after transformation using the SQL :
SELECT
        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
        TIMESTAMP_0 as timestamp_0,
        TIMESTAMP_5 as timestamp_5,
        TIMESTAMP_7 as timestamp_7,
        TIMESTAMP_9 as timestamp_9,
        TIMESTAMP0_WITH_TZ as timestamp0_with_tz,TIMESTAMP0_WITH_TZ_utc as timestamp0_with_tz_utc,
        TIMESTAMP5_WITH_TZ as timestamp5_with_tz,TIMESTAMP5_WITH_TZ_utc as timestamp5_with_tz_utc,
        TIMESTAMP8_WITH_TZ as timestamp8_with_tz,TIMESTAMP8_WITH_TZ_utc as timestamp8_with_tz_utc
        from __source__

+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-04 17:35:50.818643|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-04 17:35:50.818643|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+

21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: Writing data to target table: pgdb.ora_to_postgres
21/05/04 17:35:56 INFO targetjdbc.JDBCtoJDBC: Showing data in target table  : pgdb.ora_to_postgres
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-04 17:35:52.709503|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-04 17:35:52.709503|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
```
More connectors from other JDBC sources to JDBC destination are avaialble [here](https://romans-weapon.github.io/spear-framework/#jdbc-source).


### Streaming source

#### Kafka to Postgres Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val targetParams = Map(
  "driver" -> "org.postgresql.Driver",
  "user" -> "postgres_user",
  "password" -> "mysecretpassword",
  "url" -> "jdbc:postgresql://localhost:5432/pgdb"
)

val streamTOPostgres=SpearConnector
   .createConnector(name="StreamKafkaToPostgresconnector")
   .source(sourceType = "stream",sourceFormat = "kafka")
   .target(targetType = "relational",targetFormat = "jdbc")
   .getConnector

val schema = StructType(
    Array(StructField("id", StringType),
      StructField("name", StringType)
    ))

streamTOPostgres
    .source(sourceObject = "stream_topic",Map("kafka.bootstrap.servers"-> "kafka:9092","failOnDataLoss"->"true","startingOffsets"-> "earliest"),schema)
    .saveAs("__tmp2__")
    .transformSql("select cast (id as INT) as id, name from __tmp2__")
    .targetJDBC(objectName="person", params=targetParams, SaveMode.Append)

streamTOPostgres.stop()
```

### Target FS (HDFS)

#### Postgres to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
import java.util.Properties

Logger.getLogger("com.github").setLevel(Level.INFO)

val postgresToHiveConnector = SpearConnector
  .createConnector(name="PostgrestoHiveConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .getConnector
  
postgresToHiveConnector.setVeboseLogging(true)  

postgresToHiveConnector
  .source("source_db.instance", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres", "password" -> "test", "url" -> "jdbc:postgresql://postgres-host:5433/source_db"))
  .saveAs("__tmp__")
  .transformSql(
    """
      |select cast( uuid as string) as uuid,
      |cast( type_id as bigint ) as type_id, 
      |cast( factory_message_process_id as bigint) as factory_message_process_id,
      |cast( factory_uuid as string ) as factory_uuid,
      |cast( factory_id as bigint ) as factory_id,
      |cast( engine_id as bigint ) as engine_id,
      |cast( topic as string ) as topic,
      |cast( status_code_id as int) as status_code_id,
      |cast( cru_by as string ) as cru_by,cast( cru_ts as timestamp) as cru_ts 
      |from __tmp__""".stripMargin)
  .targetFS(destinationFilePath = "/tmp/ingest_test.db", saveAsTable = "ingest_test.postgres_data", saveMode=SaveMode.Overwrite)

postgresToHiveConnector.stop()
```

### Output

```commandline
21/05/01 10:39:20 INFO targetFS.JDBCtoFS: Reading source data from table: source_db.instance
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                 |type_id    |factory_message_process_id   |factory_uuid                        |factory_id    |   engine_id      |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |ABCDE |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |ABCDE |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |ABCDE |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_authtoken           |5             |ABCDE |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_authtoken           |5             |ABCDE |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_authtoken           |5             |ABCDE |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_error              |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_error               |5             |ABCDE |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_flowcontroller      |5             |ABCDE |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows

21/05/01 10:39:31 INFO targetFS.JDBCtoFS: Data is saved as a temporary table by name: __tmp__
21/05/01 10:39:31 INFO targetFS.JDBCtoFS: showing saved data from temporary table with name: __tmp__
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                |type_id    |factory_message_process_id    |factory_uuid                        |factory_id    | engine_id        |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |ABCDE |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |ABCDE |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |ABCDE |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_authtoken           |5             |ABCDE |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_authtoken           |5             |ABCDE |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_authtoken           |5             |ABCDE |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_error            |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_error            |5             |ABCDE |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_flowcontroller      |5             |ABCDE |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows

21/05/01 10:39:33 INFO targetFS.JDBCtoFS: Data after transformation using the SQL :
select cast( uuid as string) as uuid,
cast( type_id as bigint ) as type_id,
cast( factory_message_process_id as bigint) as factory_message_process_id,
cast( factory_uuid as string ) as factory_uuid,
cast( factory_id as bigint ) as factory_id,
cast( workflow_engine_id as bigint ) as workflow_engine_id,
cast( topic as string ) as topic,
cast( status_code_id as int) as status_code_id,
cast( cru_by as string ) as cru_by,cast( cru_ts as timestamp) as cru_ts
from __tmp__
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                |type_id|factory_message_process_id        |factory_uuid                        |factory_id    |engine_id         |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |ABCDE |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |ABCDE |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |ABCDE |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_authtoken           |5             |ABCDE |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_authtoken           |5             |ABCDE |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_authtoken           |5             |ABCDE |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_error               |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_error               |5             |ABCDE |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_flowcontroller      |5             |ABCDE |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows

21/05/01 10:39:35 INFO targetFS.JDBCtoFS: Writing data to target file: /tmp/ingest_test.db
21/05/01 10:39:35 INFO targetFS.JDBCtoFS: Saving data to table:ingest_test.postgres_data
21/05/01 10:39:35 INFO targetFS.JDBCtoFS: Target Data in table:ingest_test.postgres_data

+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                |type_id    |factory_message_process_id    |factory_uuid                        |factory_id    |        engine_id |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |ABCDE |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |ABCDE |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |ABCDE |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_authtoken           |5             |ABCDE |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_authtoken           |5             |ABCDE |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_authtoken           |5             |ABCDE |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_error               |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_error               |5             |ABCDE |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_flowcontroller      |5             |ABCDE |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows
```

More connectors to target FileSystem HDFS are avaialable [here](https://romans-weapon.github.io/spear-framework/#target-fs-hdfs).

### Streaming source

#### Kafka to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val streamTOHdfs=SpearConnector
  .createConnector(name="StreamKafkaToPostgresconnector")
  .source(sourceType = "stream",sourceFormat = "kafka")
  .target(targetType = "FS",targetFormat = "parquet")
  .getConnector

val schema = StructType(
  Array(StructField("id", StringType),
    StructField("name", StringType)
  ))

streamTOHdfs
  .source(sourceObject = "stream_topic",Map("kafka.bootstrap.servers"-> "kafka:9092","failOnDataLoss"->"true","startingOffsets"-> "earliest"),schema)
  .saveAs("__tmp2__")
  .transformSql("select cast (id as INT), name as __tmp2__")
  .targetFS(destinationFilePath = "/tmp/ingest_test.db", saveAsTable = "ingest_test.ora_data", saveMode=SaveMode.Append)

streamTOHdfs.stop()
```



### Target FS (Cloud)

#### Oracle To S3 Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "*****")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "*****")


val oracleTOS3Connector = SpearConnector
  .createConnector("ORAtoS3")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .getConnector

oracleTOS3Connector.setVeboseLogging(true)  
oracleTOS3Connector
  .sourceSql(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "user" -> "user", "password" -> "pass", "url" -> "jdbc:oracle:thin:@ora-host:1521:orcl"),
    """
      |SELECT
      |        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
      |        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
      |        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
      |        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
      |        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
      |        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
      |        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
      |        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc
      |        from DBSRV.ORACLE_NUMBER
      |""".stripMargin)
  .saveAs("__source__")
  .transformSql(
    """
      |SELECT
      |        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
      |        TIMESTAMP_0 as timestamp_0,
      |        TIMESTAMP_5 as timestamp_5,
      |        TIMESTAMP_7 as timestamp_7,
      |        TIMESTAMP_9 as timestamp_9,
      |        TIMESTAMP0_WITH_TZ as timestamp0_with_tz,TIMESTAMP0_WITH_TZ_utc as timestamp0_with_tz_utc,
      |        TIMESTAMP5_WITH_TZ as timestamp5_with_tz,TIMESTAMP5_WITH_TZ_utc as timestamp5_with_tz_utc,
      |        TIMESTAMP8_WITH_TZ as timestamp8_with_tz,TIMESTAMP8_WITH_TZ_utc as timestamp8_with_tz_utc
      |        from __source__
      |""".stripMargin)
  .targetFS(destinationFilePath="s3a://destination/data", saveMode=SaveMode.Overwrite)

oracleTOS3Connector.stop()
```

### Output

```commandline
21/05/08 08:46:11 INFO targetFS.JDBCtoFS: Executing source sql query:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc
        from DBSRV.ORACLE_TIMESTAMPS

21/05/08 08:46:11 INFO targetFS.JDBCtoFS: Data is saved as a temporary table by name: __source__
21/05/08 08:46:11 INFO targetFS.JDBCtoFS: Showing saved data from temporary table with name: __source__
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|INGEST_TS_UTC             |TIMESTAMP_0          |TIMESTAMP_5              |TIMESTAMP_7                |TIMESTAMP_9                  |TIMESTAMP0_WITH_TZ                 |TIMESTAMP0_WITH_TZ_UTC|TIMESTAMP5_WITH_TZ                       |TIMESTAMP5_WITH_TZ_UTC   |TIMESTAMP8_WITH_TZ                          |TIMESTAMP8_WITH_TZ_UTC      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-08 08:46:12.178719|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-08 08:46:12.178719|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+

21/05/08 08:46:12 INFO targetFS.JDBCtoFS: Data after transformation using the SQL :
SELECT
        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
        TIMESTAMP_0 as timestamp_0,
        TIMESTAMP_5 as timestamp_5,
        TIMESTAMP_7 as timestamp_7,
        TIMESTAMP_9 as timestamp_9,
        TIMESTAMP0_WITH_TZ as timestamp0_with_tz,TIMESTAMP0_WITH_TZ_utc as timestamp0_with_tz_utc,
        TIMESTAMP5_WITH_TZ as timestamp5_with_tz,TIMESTAMP5_WITH_TZ_utc as timestamp5_with_tz_utc,
        TIMESTAMP8_WITH_TZ as timestamp8_with_tz,TIMESTAMP8_WITH_TZ_utc as timestamp8_with_tz_utc
        from __source__

+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-08 08:46:12.438578|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-08 08:46:12.438578|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
21/05/08 08:46:12 INFO targetFS.JDBCtoFS: Writing data to target path: s3a://destination/data
21/05/08 08:47:06 INFO targetFS.JDBCtoFS: Saving data to path:s3a://destination/data

Data at S3:
===========
user@node:~$ aws s3 ls s3://destination/data
2021-05-08 12:10:09          0 _SUCCESS
2021-05-08 12:09:59       4224 part-00000-71fad52e-404d-422c-a6af-7889691bc506-c000.snappy.parquet

```

More connectors to target FileSystem Cloud (s3/gcs/adls..ect) are avaialable [here](https://romans-weapon.github.io/spear-framework/#target-fs-cloud).

## Target NOSQL

### File source

#### CSV to MongoDB Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)
val mongoProps = Map(
  "uri" -> "mongodb://mongo:27017"
)

val csvMongoConnector = SpearConnector
  .createConnector("csv-mongo")
  .source(sourceType = "file", sourceFormat = "csv")
  .target(targetType = "nosql", targetFormat = "mongo")
  .getConnector
csvMongoConnector.setVeboseLogging(true)
csvMongoConnector
  .source(sourceObject = "file:///opt/spear-framework/data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("__tmp__")
  .transformSql(
    """select state_code,party,
      |sum(votes) as total_votes
      |from __tmp__
      |group by state_code,party""".stripMargin)
  .targetNoSQL(objectName = "ingest.csvdata", props = mongoProps, saveMode = SaveMode.Overwrite)
csvMongoConnector.stop()
```


##### Output

````commandline

21/05/30 11:18:02 INFO targetNoSQL.FilettoNoSQL: Connector:csv-mongo to Target:NoSQL DB with Format:mongo from Source:file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with Format:csv started running !!
21/05/30 11:18:04 INFO targetNoSQL.FilettoNoSQL: Reading source file: file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with format: csv status:success
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|country_id|state_code|country_name|country_total_votes|party|first_name|last_name|votes|
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|1         |AK        |Alasaba     |220596             |Dem  |Barack    |Obama    |91696|
|2         |AK        |Akaskak     |220596             |Dem  |Barack    |Obama    |91696|
|3         |AL        |Autauga     |23909              |Dem  |Barack    |Obama    |6354 |
|4         |AK        |Akaska      |220596             |Dem  |Barack    |Obama    |91696|
|5         |AL        |Baldwin     |84988              |Dem  |Barack    |Obama    |18329|
|6         |AL        |Barbour     |11459              |Dem  |Barack    |Obama    |5873 |
|7         |AL        |Bibb        |8391               |Dem  |Barack    |Obama    |2200 |
|8         |AL        |Blount      |23980              |Dem  |Barack    |Obama    |2961 |
|9         |AL        |Bullock     |5318               |Dem  |Barack    |Obama    |4058 |
|10        |AL        |Butler      |9483               |Dem  |Barack    |Obama    |4367 |
+----------+----------+------------+-------------------+-----+----------+---------+-----+
only showing top 10 rows

21/05/30 11:18:04 INFO targetNoSQL.FilettoNoSQL: Saving data as temporary table:__tmp__ success
21/05/30 11:18:04 INFO targetNoSQL.FilettoNoSQL: Executing tranformation sql: select state_code,party,
sum(votes) as total_votes
from __tmp__
group by state_code,party status :success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

21/05/30 11:18:08 INFO targetNoSQL.FilettoNoSQL: Write data to object ingest.csvdata completed with status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

````
Other connectors with NO-SQL as destination are avaialble [here](https://romans-weapon.github.io/spear-framework/#target-nosql).



## Target GraphDB
Spear framework is also provisioned to write connectors with graph databases as targets from different sources.This section has the example connectors form different source to graph databases.The target properties or options for writing to graph databas as target can be refered from [here](https://neo4j.com/developer/spark/writing/)

### File Source

#### CSV to Neo4j Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

Logger.getLogger("com.github").setLevel(Level.INFO)

val neo4jParams = Map("url" -> "bolt://host:7687",
    "authentication.basic.username" -> "neo4j",
    "authentication.basic.password" -> "****"
  )

val csvtoNeo4j = SpearConnector
    .createConnector("CSV-to-Neo4j")
    .source("file", "csv")
    .target("graph", "neo4j")
    .getConnector
csvtoNeo4j.setVeboseLogging(true)

csvtoNeo4j
    .source(sourceObject = "file:///opt/spear-framework/data/FinancialSample.csv", Map("header" -> "true", "inferSchema" -> "true"))
    .saveAs("__STAGE__")
    .transformSql(
      """
        |select Segment,Country,Product
        |`Units Sold`,`Manufacturing Price`
        |from __STAGE__""".stripMargin)
    .targetGraphDB(objectName = "finance", props = neo4jParams, saveMode = SaveMode.Overwrite)
csvtoNeo4j.stop()    
```

##### Output
```commandline
21/06/17 13:03:51 INFO targetGraphDB.FiletoGraphDB: Connector:CSV-to-Neo4j to Target:GraphDB with Format:neo4j from Source:file:///opt/spear-framework/data/FinancialSample.csv with Format:csv started running !!
21/06/17 13:03:51 INFO targetGraphDB.FiletoGraphDB: Reading source file: file:///opt/spear-framework/data/FinancialSample.csv with format: csv status:success
+----------------+-------+-----------+-------------+----------+-------------------+----------+------------+---------+------------+------------+------------+---------+------------+------------+----+
|Segment         |Country|Product    |Discount Band|Units Sold|Manufacturing Price|Sale Price|Gross Sales |Discounts|Sales       |COGS        |Profit      |Date     |Month Number| Month Name |Year|
+----------------+-------+-----------+-------------+----------+-------------------+----------+------------+---------+------------+------------+------------+---------+------------+------------+----+
|Government      |Canada | Carretera | None        | 1,618.50 |3.0                |20.0      | 32,370.00  | -       | 32,370.00  | 16,185.00  | 16,185.00  |1/1/2014 |1           | January    |2014|
|Government      |Germany| Carretera | None        | 1,321.00 |3.0                |20.0      | 26,420.00  | -       | 26,420.00  | 13,210.00  | 13,210.00  |1/1/2014 |1           | January    |2014|
|Midmarket       |France | Carretera | None        | 2,178.00 |3.0                |15.0      | 32,670.00  | -       | 32,670.00  | 21,780.00  | 10,890.00  |6/1/2014 |6           | June       |2014|
|Midmarket       |Germany| Carretera | None        | 888.00   |3.0                |15.0      | 13,320.00  | -       | 13,320.00  | 8,880.00   | 4,440.00   |6/1/2014 |6           | June       |2014|
|Midmarket       |Mexico | Carretera | None        | 2,470.00 |3.0                |15.0      | 37,050.00  | -       | 37,050.00  | 24,700.00  | 12,350.00  |6/1/2014 |6           | June       |2014|
|Government      |Germany| Carretera | None        | 1,513.00 |3.0                |350.0     | 529,550.00 | -       | 529,550.00 | 393,380.00 | 136,170.00 |12/1/2014|12          | December   |2014|
|Midmarket       |Germany| Montana   | None        | 921.00   |5.0                |15.0      | 13,815.00  | -       | 13,815.00  | 9,210.00   | 4,605.00   |3/1/2014 |3           | March      |2014|
|Channel Partners|Canada | Montana   | None        | 2,518.00 |5.0                |12.0      | 30,216.00  | -       | 30,216.00  | 7,554.00   | 22,662.00  |6/1/2014 |6           | June       |2014|
|Government      |France | Montana   | None        | 1,899.00 |5.0                |20.0      | 37,980.00  | -       | 37,980.00  | 18,990.00  | 18,990.00  |6/1/2014 |6           | June       |2014|
|Channel Partners|Germany| Montana   | None        | 1,545.00 |5.0                |12.0      | 18,540.00  | -       | 18,540.00  | 4,635.00   | 13,905.00  |6/1/2014 |6           | June       |2014|
+----------------+-------+-----------+-------------+----------+-------------------+----------+------------+---------+------------+------------+------------+---------+------------+------------+----+
only showing top 10 rows

21/06/17 13:03:51 INFO targetGraphDB.FiletoGraphDB: Saving data as temporary table:__STAGE__ success
21/06/17 13:03:51 INFO targetGraphDB.FiletoGraphDB: Executing transformation sql:
select Segment,Country,Product
`Units Sold`,`Manufacturing Price`
from __STAGE__ status :success
+----------------+-------+-----------+-------------------+
|Segment         |Country|Units Sold |Manufacturing Price|
+----------------+-------+-----------+-------------------+
|Government      |Canada | Carretera |3.0                |
|Government      |Germany| Carretera |3.0                |
|Midmarket       |France | Carretera |3.0                |
|Midmarket       |Germany| Carretera |3.0                |
|Midmarket       |Mexico | Carretera |3.0                |
|Government      |Germany| Carretera |3.0                |
|Midmarket       |Germany| Montana   |5.0                |
|Channel Partners|Canada | Montana   |5.0                |
|Government      |France | Montana   |5.0                |
|Channel Partners|Germany| Montana   |5.0                |
+----------------+-------+-----------+-------------------+
only showing top 10 rows

21/06/17 13:03:52 INFO targetGraphDB.FiletoGraphDB: Write data to object:finance completed with status:success
+----------------+-------+-----------+-------------------+
|Segment         |Country|Units Sold |Manufacturing Price|
+----------------+-------+-----------+-------------------+
|Government      |Canada | Carretera |3.0                |
|Government      |Germany| Carretera |3.0                |
|Midmarket       |France | Carretera |3.0                |
|Midmarket       |Germany| Carretera |3.0                |
|Midmarket       |Mexico | Carretera |3.0                |
|Government      |Germany| Carretera |3.0                |
|Midmarket       |Germany| Montana   |5.0                |
|Channel Partners|Canada | Montana   |5.0                |
|Government      |France | Montana   |5.0                |
|Channel Partners|Germany| Montana   |5.0                |
+----------------+-------+-----------+-------------------+
only showing top 10 rows
```
Other connectors with graph db as destination are avaialble [here](https://romans-weapon.github.io/spear-framework/#target-graphdb).


## Other Functionalities of Spear
This section describes other functionalities which you can use with spear

### Merge using executeQuery API
When you want to merge or join two sources of the same type and then tranform and load the resultant data you can use the executeQuery() function of spear.Below is the example

```scala

import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)
val postgresToHiveConnector = SpearConnector
  .createConnector("PostgresToHiveConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .getConnector

postgresToHiveConnector
  .source("table1", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("tmp")

postgresToHiveConnector
  .source("table2", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("tmp2")
  

postgresToHiveConnector.executeQuery(
  """
  //execute join query between the loaded sources
   """.stripMargin)
  .saveAs("result")
  .transformSql(
    """
    //tranform sql
    """.stripMargin)
  .targetFS(destinationFilePath = "/tmp/ingest", saveAsTable = "target", saveMode = SaveMode.Overwrite)
```
See more detailed explanation about executeQuery AP1 with diagrams [here](https://romans-weapon.github.io/spear-framework/#merge-using-executequery-api)

### Write to multi-targets using branch API
While using multitarget api make sure you are specifying thet target type while defining destination.

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

Logger.getLogger("com.github").setLevel(Level.INFO)
val properties = new Properties()
properties.put("driver", "org.postgresql.Driver")
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres:5432/pgdb")

val mongoProps = new Properties()
mongoProps.put("uri", "mongodb://mongodb:27017")

val csvMultiTargetConnector = SpearConnector
  .createConnector("CSV-Any")
  .source(sourceType = "file", sourceFormat = "csv")
  .multiTarget  //For multi target use multitarget intsead of target and provide the dest-format along with the target definition in the connector logic
  .getConnector

csvMultiTargetConnector.setVeboseLogging(true)

csvMultiTargetConnector
  .source(sourceObject = "file:///opt/spear-framework/data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("_table_")
  .branch
  .targets(
     csvMultiTargetConnector.targetFS(destinationFilePath = "", destFormat = "parquet", saveAsTable = "ingest.raw", saveMode = SaveMode.Overwrite) -- //target -1
    //target -2
    ....
    //target -n
  )

```
See more detailed explanation about multi-destinations with diagrams [here](https://romans-weapon.github.io/spear-framework/#multi-targets-using-branch-api)


## Contributions and License
#### License
Software Licensed under the [Apache License 2.0](LICENSE)
#### Author
Anudeep Konaboina <krantianudeep@gmail.com>
#### Contributor
Kayan Deshi <kalyan.mgit@gmail.com>

## Visit Website
Watch example connectors from different sources to different targets, visit github page [here](https://romans-weapon.github.io/spear-framework/)
