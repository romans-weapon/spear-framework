
# Spear Framework - Introduction
The spear-framework provides scope to write simple ETL/ELT-connectors/pipelines for moving data from different sources to different destinations which greatly minimizes the effort of writing complex codes for data ingestion. Connectors which have the ability to extract and load (ETL or ELT) any kind of data from source with custom tansformations applied can be written and executed seamlessly using spear connectors.

![image](https://user-images.githubusercontent.com/59328701/122396653-d412a300-cf95-11eb-8bd5-bef400c07de8.png)


# Framework Design and Code Quality 

Below is the code and design quality score for Spear framework given by Code Inspector. For more details click [here](https://frontend.code-inspector.com/public/user/github/AnudeepKonaboina)

![image](https://user-images.githubusercontent.com/59328701/122229966-d661f800-ced6-11eb-839a-c77ca7cca610.png)


# Table Of Contents
- [Version Support](#version-support)
- [How to use Spear](#how-to-use-spear)
    * [SBT dependency for Spear](#sbt-dependency-for-spear)
    * [Maven dependency for Spear](#maven-dependency-for-spear)
    * [Spark shell package for Spear](#spark-shell-package-for-spear)
    * [Docker container setup for Spear](#docker-container-setup-for-spear)
- [Connectors built using spear](#connectors-built-using-spear)
    * [Target JDBC](#target-jdbc)
        - [File Source](#file-source)
            + [CSV to Postgres Connector](#csv-to-postgres-connector)
            + [JSON to Postgres Connector](#json-to-postgres-connector)
            + [XML to Postgres Connector](#xml-to-postgres-connector)
            + [Avro to Postgres Connector](#avro-to-postgres-connector)
            + [Parquet to Postgres Connector](#parquet-to-postgres-connector)
        - [JDBC Source](#jdbc-source)
            + [Oracle to Postgres Connector](#oracle-to-postgres-connector)
            + [Postgres to Salesforce Connector](#postgres-to-salesforce-connector)
            + [Hive to Postgres Connector](#hive-to-postgres-connector)
        - [NOSQL Source](#nosql-source)
            + [MongoDB to Postgres Connector](#mongodb-to-postgres-connector)
        - [Streaming Source](#streaming-source)
            + [kafka to Postgres Connector](#kafka-to-postgres-connector)
        - [GraphDB Source](#graphdb-source)
            + [Neo4j to Postgres Connector](#neo4j-to-postgres-connector)
    * [Target FS (HDFS)](#target-fs-hdfs)
        - [JDBC Source](#jdbc-source)
            + [Postgres to Hive Connector](#postgres-to-hive-connector)
            + [Oracle to Hive Connector](#oracle-to-hive-connector)
            + [Salesforce to Hive Connector](#salesforce-to-hive-connector)
        - [Streaming Source](#streaming-source)
            + [kafka to Hive Connector](#kafka-to-hive-connector)
        - [NOSQL Source](#nosql-source)
            + [MongoDB to Hive Connector](#mongodb-to-hive-connector)
        - [Cloud Source](#cloud-source)
            + [S3 to Hive Connector](#s3-to-hive-connector)  
    * [Target FS (Cloud)](#target-fs-cloud)
        - [JDBC Source](#jdbc-source)
            + [Oracle to S3 Connector](#oracle-to-s3-connector)
            + [Postgres to GCS Connector](#postgres-to-gcs-connector)
            + [Salesforce to S3 Connector](#salesforce-to-s3-connector)
    * [Target NOSQL](#target-nosql) 
         - [File Source](#file-source)
             + [CSV to MongoDB Connector](#csv-to-mongodb-connector)
         - [JDBC Source](#jdbc-source)
             + [Salesforce to Cassandra Connector](#salesforce-to-cassandra-connector)
         - [NOSQL Source](#nosql-source)
             + [Cassandra to Mongo Connector](#cassandra-to-mongo-connector)
    * [Target GraphDB](#target-graphdb)
         - [File Source](#file-source)
             + [CSV to Neo4j Connector](#csv-to-neo4j-connector)
         - [JDBC Source](#jdbc-source)
             + [Postgres to Neo4j Connector](#postgres-to-neo4j-connector)
         - [NOSQL Source](#nosql-source)
             + [MongoDB to Neo4j Connector](#mongodb-to-neo4j-connector)
- [Other Functionalities of Spear](#other-functionalities-of-spear)
    * [Merge using executeQuery API](#merge-using-executequery-api) 
         - [Postgres to Hive Connector With executeQuery AP1](#postgres-to-hive-connector-with-executequery-api)
    * [Multi-targets using branch API](#multi-targets-using-branch-api)
         - [CSV to Multi-Target Connector With branch API](#csv-to-multi-target-connector-with-branch-api)
    * [Merge and batch API combination](#merge-and-batch-api-combination) 
         - [Multi-source to Multi-Target Connector](#multi-source-to-multi-target-connector)

# Version Support
Below are the stable releases and their respective versions supported by spear-framework:

| stable-version               | spark-version    | scala-version  | maven-release-version            |
| ---------------------------  | ---------------- |--------------- |----------------------------------|
| v0.9-2.4.7-2.11              | 2.4.7,2.4.x      | 2.11           | 3.0.1                            |
| v0.1-3.1.1-2.12              | 3.1.1,3.1.x      | 2.12           | 1.0                              |

# How to use Spear

### SBT dependency for Spear

You can add spear-framework as dependency in your projects **build.sbt** file as show below

#### For spark-2.4.7
```commandline
libraryDependencies += "io.github.romans-weapon" %% "spear-framework" % "2.4-3.0.1"
```

#### For spark-3.1.1
```commandline
libraryDependencies += "io.github.romans-weapon" % "spear-framework_2.12" % "3.1.1-1.0"
```


### Maven dependency for Spear

#### For spark-2.4.7
```commandline
<dependency>
  <groupId>io.github.romans-weapon</groupId>
  <artifactId>spear-framework_2.11</artifactId>
  <version>2.4-3.0.1</version>
</dependency>
```

#### For spark-3.1.1
```commandline
<dependency>
  <groupId>io.github.romans-weapon</groupId>
  <artifactId>spear-framework_2.12</artifactId>
  <version>3.1.1-1.0</version>
</dependency>
```

### Spark shell package for Spear

You can also add it as a maven package while starting spark-shell .

#### For spark-2.4.7
```commandline
spark-shell --packages "io.github.romans-weapon:spear-framework_2.11:2.4-3.0.1"
```

#### For spark-3.1.1
```commandline
spark-shell --packages "io.github.romans-weapon:spear-framework_2.12:3.1.1-1.0"

```



### Docker container setup for Spear
Below are the simple steps to setup spear on any machine having docker and docker-compose pre-installed :

1. Clone the repository from git and navigate to project directory
#### For spark-2.4.7:
```commandline
git clone -b main https://github.com/AnudeepKonaboina/spear-framework.git && cd spear-framework
```
#### For spark-3.1.1:
```commandline
git clone -b spark-3.1.1 https://github.com/AnudeepKonaboina/spear-framework.git && cd spear-framework
```

2. Run setup.sh script using the command
```commandline
sh setup.sh
```

3. Once the setup is completed run the below command for entering into the container
```commandline
user@node~$ docker exec -it spear bash
```

4. Run `spear-shell` inside the conatiner to start spark shell integrated with spear .
```
root@hadoop # spear-shell
```
5. Once you enter into the conatiner you will get default hadoop/hive environment readily available to read data from any source and write it to HDFS so that it gives you complete environment to create your own data-pipelines using spear-framework.
### Services and their corresponding versions available within the container are shown below:
#### For spark-2.4.7 

| Service      | Version     |
| -----------  | ----------- |
| Spark        | 2.4.7       |
| Hadoop       | 2.10.1      |
| Hive         | 2.1.1       |

#### For spark-3.1.1

| Service      | Version     |
| -----------  | ----------- |
| Spark        | 3.1.1       |
| Hadoop       | 3.2.0       |
| Hive         | 3.1.1       |

Also it has a postgres database and a NO-SQL database mongodb as well which you can use it as a source or as a desination for writing and testing your connector.

6. Start writing your own connectors and explore .To understand how to write a connector [click here](develop-your-first-connector-using-spear)

# Connectors built using Spear
Connector is basically the logic/code which allows you to create a pipeline from source to target using the spear framework using which you can ingest data from any source to any destination.

![image](https://user-images.githubusercontent.com/59328701/119258939-7afb5d80-bbe9-11eb-837f-02515cb7cf74.png)


## Target JDBC
Spear framework supports writing data to any RDBMS with jdbc as destination(postgres/oracle/msql etc..)  from various sources like a file(csv/json/filesystem etc..)/database(RDBMS/cloud db etc..)/streaming(kafka/dir path etc..).Given below are examples of few connectors with JDBC as target.Below examples are written for postgresql as JDBC target,but this can be extended for any jdbc target.

### File source

![image](https://user-images.githubusercontent.com/59328701/119256610-2521b800-bbdf-11eb-8a82-2fd81f48e85a.png)


#### CSV to Postgres Connector
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

#### JSON to Postgres Connector

Connector for reading json file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/data.json

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
val jsonJdbcConnector = SpearConnector
  .createConnector(name = "JSONtoPostgresConnector")
  .source(sourceType = "file", sourceFormat = "json")
  .target(targetType = "relational", targetFormat = "jdbc")
  .getConnector

jsonJdbcConnector.setVeboseLogging(true)

jsonJdbcConnector
  .source("file:///opt/spear-framework/data/data.json", Map("multiline" -> "true"))
  .saveAs("__tmptable__")
  .transformSql("select cast(id*10 as integer) as type_id,type from __tmptable__ ")
  .targetJDBC(objectName = "json_to_jdbc", props = targetProps, saveMode = SaveMode.Overwrite)
```

##### Output

```
21/06/17 08:20:07 INFO targetjdbc.FiletoJDBC: ConnectorJSONtoPostgresConnector to Target:JDBC with Format:jdbc from Source:file:///opt/spear-framework/data/data.json with Format:json started running !!
21/06/17 08:20:07 INFO targetjdbc.FiletoJDBC: Reading source file: file:///opt/spear-framework/data/data.json with format:json status:success
+----+------------------------+
|id  |type                    |
+----+------------------------+
|5001|None                    |
|5002|Glazed                  |
|5005|Sugar                   |
|5007|Powdered Sugar          |
|5006|Chocolate with Sprinkles|
|5003|Chocolate               |
|5004|Maple                   |
+----+------------------------+

21/06/17 08:20:08 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:__tmptable__ success
21/06/17 08:20:08 INFO targetjdbc.FiletoJDBC: Executing transformation sql: select cast(id*10 as integer) as type_id,type from __tmptable__  status :success
+-------+------------------------+
|type_id|type                    |
+-------+------------------------+
|50010  |None                    |
|50020  |Glazed                  |
|50050  |Sugar                   |
|50070  |Powdered Sugar          |
|50060  |Chocolate with Sprinkles|
|50030  |Chocolate               |
|50040  |Maple                   |
+-------+------------------------+

21/06/17 08:20:08 INFO targetjdbc.FiletoJDBC: Write data to table/object:json_to_jdbc completed with status:success
+-------+------------------------+
|type_id|type                    |
+-------+------------------------+
|50010  |None                    |
|50020  |Glazed                  |
|50050  |Sugar                   |
|50070  |Powdered Sugar          |
|50060  |Chocolate with Sprinkles|
|50030  |Chocolate               |
|50040  |Maple                   |
+-------+------------------------+

```

#### XML to Postgres Connector

Connector for reading xml file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/data.xml

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
val xmlJdbcConnector = SpearConnector
  .createConnector(name="JSONtoPostgresConnector")
  .source(sourceType = "file", sourceFormat = "xml")
  .target(targetType = "relational", targetFormat = "jdbc")
  .getConnector

xmlJdbcConnector.setVeboseLogging(true)

xmlJdbcConnector
  .source("file:///opt/spear-framework/data/data.xml", Map("rootTag" -> "employees", "rowTag" -> "details"))
  .saveAs("tmp")
  .transformSql("select * from tmp ")
  .targetJDBC(objectName="xml_to_jdbc", props=targetProps, saveMode=SaveMode.Overwrite)

xmlJdbcConnector.stop()
```

##### Output

```
21/06/17 08:22:35 INFO targetjdbc.FiletoJDBC: Connector:JSONtoPostgresConnector to Target:JDBC with Format:jdbc from Source:file:///opt/spear-framework/data/data.xml with Format:xml started running !!
21/06/17 08:22:35 INFO targetjdbc.FiletoJDBC: Reading source file: file:///opt/spear-framework/data/data.xml with format: xml status:success
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+

21/06/17 08:22:35 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:tmp success
21/06/17 08:22:35 INFO targetjdbc.FiletoJDBC: Executing transformation sql: select * from tmp  status :success
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+

21/06/17 08:22:36 INFO targetjdbc.FiletoJDBC: Write data to table/object:xml_to_jdbc completed with status:success
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+
```

#### Avro to Postgres Connector

Connector for reading avro file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/sample_data.avro

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

val avroJdbcConnector = SpearConnector
  .createConnector(name = "AvrotoPostgresConnector")
  .source(sourceType = "file", sourceFormat = "avro")
  .target(targetType = "relational", targetFormat = "jdbc")
  .getConnector
avroJdbcConnector.setVeboseLogging(true)
avroJdbcConnector
  .source(sourceObject = "file:///opt/spear-framework/data/sample_data.avro")
  .saveAs("__tmp__")
  .transformSql(
    """select id,
      |cast(concat(first_name ,' ', last_name) as VARCHAR(255)) as name,
      |coalesce(gender,'NA') as gender,
      |cast(country as VARCHAR(20)) as country,
      |cast(salary as DOUBLE) as salary,email
      |from __tmp__""".stripMargin)
  .saveAs("__transformed_table__")
  .transformSql(
    """
      |select id,name,
      |country,email,
      |salary
      |from __transformed_table__""".stripMargin)
  .targetJDBC(objectName = "avro_data", props = targetProps, saveMode = SaveMode.Overwrite)

avroJdbcConnector.stop()
```

##### Output

```
21/06/17 08:25:37 INFO targetjdbc.FiletoJDBC: Connector:AvrotoPostgresConnector to Target:JDBC with Format:jdbc from Source:file:///opt/spear-framework/data/sample_data.avro with Format:avro started running !!
21/06/17 08:25:37 INFO targetjdbc.FiletoJDBC: Reading source file: file:///opt/spear-framework/data/sample_data.avro with format:avro status:success
+--------------------+---+----------+---------+------------------------+------+--------------+----------------+----------------------+----------+---------+------------------------+--------+
|registration_dttm   |id |first_name|last_name|email                   |gender|ip_address    |cc              |country               |birthdate |salary   |title                   |comments|
+--------------------+---+----------+---------+------------------------+------+--------------+----------------+----------------------+----------+---------+------------------------+--------+
|2016-02-03T07:55:29Z|1  |Amanda    |Jordan   |ajordan0@com.com        |Female|1.197.201.2   |6759521864920116|Indonesia             |3/8/1971  |49756.53 |Internal Auditor        |1E+02   |
|2016-02-03T17:04:03Z|2  |Albert    |Freeman  |afreeman1@is.gd         |Male  |218.111.175.34|null            |Canada                |1/16/1968 |150280.17|Accountant IV           |        |
|2016-02-03T01:09:31Z|3  |Evelyn    |Morgan   |emorgan2@altervista.org |Female|7.161.136.94  |6767119071901597|Russia                |2/1/1960  |144972.51|Structural Engineer     |        |
|2016-02-03T12:36:21Z|4  |Denise    |Riley    |driley3@gmpg.org        |Female|140.35.109.83 |3576031598965625|China                 |4/8/1997  |90263.05 |Senior Cost Accountant  |        |
|2016-02-03T05:05:31Z|5  |Carlos    |Burns    |cburns4@miitbeian.gov.cn|      |169.113.235.40|5602256255204850|South Africa          |          |null     |                        |        |
|2016-02-03T07:22:34Z|6  |Kathryn   |White    |kwhite5@google.com      |Female|195.131.81.179|3583136326049310|Indonesia             |2/25/1983 |69227.11 |Account Executive       |        |
|2016-02-03T08:33:08Z|7  |Samuel    |Holmes   |sholmes6@foxnews.com    |Male  |232.234.81.197|3582641366974690|Portugal              |12/18/1987|14247.62 |Senior Financial Analyst|        |
|2016-02-03T06:47:06Z|8  |Harry     |Howell   |hhowell7@eepurl.com     |Male  |91.235.51.73  |null            |Bosnia and Herzegovina|3/1/1962  |186469.43|Web Developer IV        |        |
|2016-02-03T03:52:53Z|9  |Jose      |Foster   |jfoster8@yelp.com       |Male  |132.31.53.61  |null            |South Korea           |3/27/1992 |231067.84|Software Test Engineer I|1E+02   |
|2016-02-03T18:29:47Z|10 |Emily     |Stewart  |estewart9@opensource.org|Female|143.28.251.245|3574254110301671|Nigeria               |1/28/1997 |27234.28 |Health Coach IV         |        |
+--------------------+---+----------+---------+------------------------+------+--------------+----------------+----------------------+----------+---------+------------------------+--------+
only showing top 10 rows

21/06/17 08:25:37 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:__tmp__ success
21/06/17 08:25:37 INFO targetjdbc.FiletoJDBC: Executing transformation sql: select id,
cast(concat(first_name ,' ', last_name) as VARCHAR(255)) as name,
coalesce(gender,'NA') as gender,
cast(country as VARCHAR(20)) as country,
cast(salary as DOUBLE) as salary,email
from __tmp__ status :success
+---+--------------+------+----------------------+---------+------------------------+
|id |name          |gender|country               |salary   |email                   |
+---+--------------+------+----------------------+---------+------------------------+
|1  |Amanda Jordan |Female|Indonesia             |49756.53 |ajordan0@com.com        |
|2  |Albert Freeman|Male  |Canada                |150280.17|afreeman1@is.gd         |
|3  |Evelyn Morgan |Female|Russia                |144972.51|emorgan2@altervista.org |
|4  |Denise Riley  |Female|China                 |90263.05 |driley3@gmpg.org        |
|5  |Carlos Burns  |      |South Africa          |null     |cburns4@miitbeian.gov.cn|
|6  |Kathryn White |Female|Indonesia             |69227.11 |kwhite5@google.com      |
|7  |Samuel Holmes |Male  |Portugal              |14247.62 |sholmes6@foxnews.com    |
|8  |Harry Howell  |Male  |Bosnia and Herzegovina|186469.43|hhowell7@eepurl.com     |
|9  |Jose Foster   |Male  |South Korea           |231067.84|jfoster8@yelp.com       |
|10 |Emily Stewart |Female|Nigeria               |27234.28 |estewart9@opensource.org|
+---+--------------+------+----------------------+---------+------------------------+
only showing top 10 rows

21/06/17 08:25:38 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:__transformed_table__ success
21/06/17 08:25:38 INFO targetjdbc.FiletoJDBC: Executing transformation sql:
select id,name,
country,email,
salary
from __transformed_table__ status :success
+---+--------------+----------------------+------------------------+---------+
|id |name          |country               |email                   |salary   |
+---+--------------+----------------------+------------------------+---------+
|1  |Amanda Jordan |Indonesia             |ajordan0@com.com        |49756.53 |
|2  |Albert Freeman|Canada                |afreeman1@is.gd         |150280.17|
|3  |Evelyn Morgan |Russia                |emorgan2@altervista.org |144972.51|
|4  |Denise Riley  |China                 |driley3@gmpg.org        |90263.05 |
|5  |Carlos Burns  |South Africa          |cburns4@miitbeian.gov.cn|null     |
|6  |Kathryn White |Indonesia             |kwhite5@google.com      |69227.11 |
|7  |Samuel Holmes |Portugal              |sholmes6@foxnews.com    |14247.62 |
|8  |Harry Howell  |Bosnia and Herzegovina|hhowell7@eepurl.com     |186469.43|
|9  |Jose Foster   |South Korea           |jfoster8@yelp.com       |231067.84|
|10 |Emily Stewart |Nigeria               |estewart9@opensource.org|27234.28 |
+---+--------------+----------------------+------------------------+---------+
only showing top 10 rows

21/06/17 08:25:38 INFO targetjdbc.FiletoJDBC: Write data to table/object:avro_data completed with status:success
+---+--------------+----------------------+------------------------+---------+
|id |name          |country               |email                   |salary   |
+---+--------------+----------------------+------------------------+---------+
|1  |Amanda Jordan |Indonesia             |ajordan0@com.com        |49756.53 |
|2  |Albert Freeman|Canada                |afreeman1@is.gd         |150280.17|
|3  |Evelyn Morgan |Russia                |emorgan2@altervista.org |144972.51|
|4  |Denise Riley  |China                 |driley3@gmpg.org        |90263.05 |
|5  |Carlos Burns  |South Africa          |cburns4@miitbeian.gov.cn|null     |
|6  |Kathryn White |Indonesia             |kwhite5@google.com      |69227.11 |
|7  |Samuel Holmes |Portugal              |sholmes6@foxnews.com    |14247.62 |
|8  |Harry Howell  |Bosnia and Herzegovina|hhowell7@eepurl.com     |186469.43|
|9  |Jose Foster   |South Korea           |jfoster8@yelp.com       |231067.84|
|10 |Emily Stewart |Nigeria               |estewart9@opensource.org|27234.28 |
+---+--------------+----------------------+------------------------+---------+
only showing top 10 rows

```

#### Parquet to Postgres Connector

Connector for reading parquet file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/sample.parquet. Note that verbose logging is not enabled for this connector 

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

val parquetJdbcConnector = SpearConnector
  .createConnector(name="ParquettoPostgresConnector")
  .source(sourceType="file", sourceFormat="parquet")
  .target(targetType="relational", targetFormat="jdbc")
  .getConnector

parquetJdbcConnector
  .source("file:///opt/spear-framework/data/sample.parquet")
  .saveAs("__tmp__")
  .transformSql("""select flow1,occupancy1,speed1 from __tmp__""")
  .targetJDBC(objectName="user_data", props=targetProps, saveMode=SaveMode.Overwrite)

parquetJdbcConnector.stop()
```

##### Output

```
//No verbose logging and hece the output at each stage is not shown
21/06/17 08:29:57 INFO targetjdbc.FiletoJDBC: ConnectorParquettoPostgresConnector to Target:JDBC with Format:jdbc from Source:file:///opt/spear-framework/data/sample.parquet with Format:file:///opt/spear-framework/data/sample.parquet started running !!
21/06/17 08:29:57 INFO targetjdbc.FiletoJDBC: Reading source file: file:///opt/spear-framework/data/sample.parquet with format: parquet status:success
21/06/17 08:29:57 INFO targetjdbc.FiletoJDBC: Saving data as temporary table:__tmp__ success
21/06/17 08:29:57 INFO targetjdbc.FiletoJDBC: Executing transformation sql: select flow1,occupancy1,speed1 from __tmp__ status :success
21/06/17 08:29:58 INFO targetjdbc.FiletoJDBC: Write data to table/object user_data completed with status:success
```

### JDBC source

![image](https://user-images.githubusercontent.com/59328701/119256795-07088780-bbe0-11eb-9c25-4f37ff232703.png)


#### Oracle to Postgres Connector

We need to add oracle jar (ojdbc6.jar) as a package and start the spear shell for this connector to work. Ex: spark-shell --jars ojdbc6.jar ...
Also note the use of sourceSQL API in this example

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
  .targetJDBC(objectName = "ora_to_postgres", props=targetProps, saveMode=SaveMode.Overwrite)

oracleTOPostgresConnector.stop()
```

##### Output

```commandline
21/06/17 08:33:54 INFO targetjdbc.JDBCtoJDBC: Connector:OracletoPostgresConnector from Source sql:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc,
        to_char(TIMESTAMP0_WITH_LTZ) as timestamp0_with_ltz , to_char(sys_extract_utc(TIMESTAMP0_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_ltz_utc,
        to_char(TIMESTAMP5_WITH_LTZ) as timestamp5_with_ltz , to_char(sys_extract_utc(TIMESTAMP5_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_ltz_utc,
        to_char(TIMESTAMP8_WITH_LTZ) as timestamp8_with_ltz , to_char(sys_extract_utc(TIMESTAMP8_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_ltz_utc
        from DBSRV.ORACLE_TIMESTAMPS
 with Format: jdbc started running!!
21/06/17 08:33:55 INFO targetjdbc.JDBCtoJDBC: Executing source sql query:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        to_char(TIMESTAMP_0, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_0,
        to_char(TIMESTAMP_5, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_5,
        to_char(TIMESTAMP_7, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_7,
        to_char(TIMESTAMP_9, 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp_9,
        to_char(TIMESTAMP0_WITH_TZ) as timestamp0_with_tz , to_char(sys_extract_utc(TIMESTAMP0_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_tz_utc,
        to_char(TIMESTAMP5_WITH_TZ) as timestamp5_with_tz , to_char(sys_extract_utc(TIMESTAMP5_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_tz_utc,
        to_char(TIMESTAMP8_WITH_TZ) as timestamp8_with_tz , to_char(sys_extract_utc(TIMESTAMP8_WITH_TZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_tz_utc,
        to_char(TIMESTAMP0_WITH_LTZ) as timestamp0_with_ltz , to_char(sys_extract_utc(TIMESTAMP0_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS') as timestamp0_with_ltz_utc,
        to_char(TIMESTAMP5_WITH_LTZ) as timestamp5_with_ltz , to_char(sys_extract_utc(TIMESTAMP5_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp5_with_ltz_utc,
        to_char(TIMESTAMP8_WITH_LTZ) as timestamp8_with_ltz , to_char(sys_extract_utc(TIMESTAMP8_WITH_LTZ), 'YYYY-MM-DD HH24:MI:SS.FF') as timestamp8_with_ltz_utc
        from DBSRV.ORACLE_TIMESTAMPS
 with format: jdbc status:success
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|INGEST_TS_UTC             |TIMESTAMP_0          |TIMESTAMP_5              |TIMESTAMP_7                |TIMESTAMP_9                  |TIMESTAMP0_WITH_TZ                 |TIMESTAMP0_WITH_TZ_UTC|TIMESTAMP5_WITH_TZ                       |TIMESTAMP5_WITH_TZ_UTC   |TIMESTAMP8_WITH_TZ                          |TIMESTAMP8_WITH_TZ_UTC      |TIMESTAMP0_WITH_LTZ  |TIMESTAMP0_WITH_LTZ_UTC|TIMESTAMP5_WITH_LTZ        |TIMESTAMP5_WITH_LTZ_UTC  |TIMESTAMP8_WITH_LTZ           |TIMESTAMP8_WITH_LTZ_UTC     |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|2021-06-17 08:33:55.587670|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|07-APR-21 09.45.16 AM|2021-04-07 09:45:16    |07-APR-21 09.45.16.03356 AM|2021-04-07 09:45:16.03356|07-APR-21 09.45.16.03356100 AM|2021-04-07 09:45:16.03356100|
|2021-06-17 08:33:55.587670|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|07-APR-21 09.46.52 AM|2021-04-07 09:46:52    |07-APR-21 09.46.51.60911 AM|2021-04-07 09:46:51.60911|07-APR-21 09.46.51.60910900 AM|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+

21/06/17 08:33:55 INFO targetjdbc.JDBCtoJDBC: Saving data as temporary table:__source__ success
21/06/17 08:33:55 INFO targetjdbc.JDBCtoJDBC: Executing transformation sql:
SELECT
        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
        TIMESTAMP_0 as timestamp_0,
        TIMESTAMP_5 as timestamp_5,
        TIMESTAMP_7 as timestamp_7,
        TIMESTAMP_9 as timestamp_9,
        TIMESTAMP0_WITH_TZ as timestamp0_with_tz,TIMESTAMP0_WITH_TZ_utc as timestamp0_with_tz_utc,
        TIMESTAMP5_WITH_TZ as timestamp5_with_tz,TIMESTAMP5_WITH_TZ_utc as timestamp5_with_tz_utc,
        TIMESTAMP8_WITH_TZ as timestamp8_with_tz,TIMESTAMP8_WITH_TZ_utc as timestamp8_with_tz_utc,
        TIMESTAMP0_WITH_LTZ as timestamp0_with_ltz,TIMESTAMP0_WITH_LTZ_utc as timestamp0_with_ltz_utc,
        TIMESTAMP5_WITH_LTZ as timestamp5_with_ltz,TIMESTAMP5_WITH_LTZ_utc as timestamp5_with_ltz_utc,
        TIMESTAMP8_WITH_LTZ as timestamp8_with_ltz,TIMESTAMP8_WITH_LTZ_utc as timestamp8_with_ltz_utc
        from __source__
 status :success
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |timestamp0_with_ltz  |timestamp0_with_ltz_utc|timestamp5_with_ltz        |timestamp5_with_ltz_utc  |timestamp8_with_ltz           |timestamp8_with_ltz_utc     |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|2021-06-17 08:33:55.879345|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|07-APR-21 09.45.16 AM|2021-04-07 09:45:16    |07-APR-21 09.45.16.03356 AM|2021-04-07 09:45:16.03356|07-APR-21 09.45.16.03356100 AM|2021-04-07 09:45:16.03356100|
|2021-06-17 08:33:55.879345|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|07-APR-21 09.46.52 AM|2021-04-07 09:46:52    |07-APR-21 09.46.51.60911 AM|2021-04-07 09:46:51.60911|07-APR-21 09.46.51.60910900 AM|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+

21/06/17 08:33:56 INFO targetjdbc.JDBCtoJDBC: Write data to table/object:ora_to_postgres completed with status:success
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |timestamp0_with_ltz  |timestamp0_with_ltz_utc|timestamp5_with_ltz        |timestamp5_with_ltz_utc  |timestamp8_with_ltz           |timestamp8_with_ltz_utc     |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+
|2021-06-17 08:33:56.515644|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|07-APR-21 09.45.16 AM|2021-04-07 09:45:16    |07-APR-21 09.45.16.03356 AM|2021-04-07 09:45:16.03356|07-APR-21 09.45.16.03356100 AM|2021-04-07 09:45:16.03356100|
|2021-06-17 08:33:56.515644|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|07-APR-21 09.46.52 AM|2021-04-07 09:46:52    |07-APR-21 09.46.51.60911 AM|2021-04-07 09:46:51.60911|07-APR-21 09.46.51.60910900 AM|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+---------------------+-----------------------+---------------------------+-------------------------+------------------------------+----------------------------+


```

#### Postgres to Salesforce Connector
While writing to Salesforce,below are the pre-requisites one must take care of:
1. A salesforce object(or)dataset must be available at Salesforce before writing data into it as spark will not create an object automatically and your connector might fail.
2. The columns/fields in source data (or) dataframe must match the columns/fileds in the destination salesforce object.
3. Reference used for building this connector: [spark-salesforce](https://github.com/springml/spark-salesforce)

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val targetSalesForceProps=Map(
  "login"-> "https://login.salesforce.com",
  "username" -> "user",
  "password" -> "pass<token>"
)

val postgresSalesForce = SpearConnector
  .createConnector("Postgres-Salesforce Connectoer")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "relational", targetFormat = "soql")
  .getConnector

postgresSalesForce.setVeboseLogging(true)

postgresSalesForce
  .source(sourceObject = "employee_details", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("__tmp__")
  .transformSql(
    """
      |select emp_id as Employee_id__c,
      |emp_name as Name,
      |emp_add as Employee_address__c,
      |emp_phone as Employee_ph__c
      |from __tmp__""".stripMargin)
  .targetJDBC(objectName = "Employee__c", props=targetSalesForceProps, saveMode=SaveMode.Overwrite)

postgresSalesForce.stop()
```

##### Output
```commandline
21/06/17 09:49:43 INFO targetjdbc.JDBCtoJDBC: Connector:Postgres-Salesforce-Connector to Target:JDBC with Format:soql from Source table/Object:employee_details with Format:jdbc started running!!
21/06/17 09:49:43 INFO targetjdbc.JDBCtoJDBC: Reading source table:employee_details with format:jdbc status:success
+------+--------+-------+---------+
|emp_id|emp_name|emp_add|emp_phone|
+------+--------+-------+---------+
|1     |Ram     |HYD    |1234.0   |
|2     |Ravi    |SEC    |12345.0  |
|3     |Ramesh  |SEC    |345.0    |
|4     |Rao     |BKP    |3445.0   |
|5     |Rahul   |SKP    |344567.0 |
|6     |Raghu   |KP     |231567.0 |
|7     |Ratan   |MGBS   |131567.0 |
+------+--------+-------+---------+

21/06/17 09:49:43 INFO targetjdbc.JDBCtoJDBC: Saving data as temporary table:__tmp__ success
21/06/17 09:49:43 INFO targetjdbc.JDBCtoJDBC: Executing transformation sql:
select emp_id as  Employee_id__c,
emp_name as Name,
emp_add as Employee_address__c,
emp_phone as Employee_ph__c
from __tmp__ status :success
+--------------+------+-------------------+--------------+
|Employee_id__c|Name  |Employee_address__c|Employee_ph__c|
+--------------+------+-------------------+--------------+
|1             |Ram   |HYD                |1234.0        |
|2             |Ravi  |SEC                |12345.0       |
|3             |Ramesh|SEC                |345.0         |
|4             |Rao   |BKP                |3445.0        |
|5             |Rahul |SKP                |344567.0      |
|6             |Raghu |KP                 |231567.0      |
|7             |Ratan |MGBS               |131567.0      |
+--------------+------+-------------------+--------------+

21/06/17 09:49:47 INFO targetjdbc.JDBCtoJDBC: Write data to table/object:Employee__c completed with status:success
+--------------+------+-------------------+--------------+
|Employee_id__c|Name  |Employee_address__c|Employee_ph__c|
+--------------+------+-------------------+--------------+
|1             |Ram   |HYD                |1234.0        |
|2             |Ravi  |SEC                |12345.0       |
|3             |Ramesh|SEC                |345.0         |
|4             |Rao   |BKP                |3445.0        |
|5             |Rahul |SKP                |344567.0      |
|6             |Raghu |KP                 |231567.0      |
|7             |Ratan |MGBS               |131567.0      |
+--------------+------+-------------------+--------------+
```

#### Hive to postgres connector
This conector is used to read data from native hive and writing it to postgres.\
When working with Hive, one must instantiate SparkSession with Hive support.Spear does all that internally.\
Spark connects to the Hive metastore directly via a HiveContext It does not use JDBC.The configuration of Hive is done by placing your hive-site.xml, core-site.xml (for security configuration), and hdfs-site.xml (for HDFS configuration) file in conf/ folder of spark.

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

val properties = new Properties()
  properties.put("driver", "org.postgresql.Driver");
  properties.put("user", "postgres_user")
  properties.put("password", "mysecretpassword")
  properties.put("url", "jdbc:postgresql://postgres:5432/pgdb")

Logger.getLogger("com.github").setLevel(Level.INFO)

val hiveJdbcConnector = SpearConnector
    .createConnector("HIVETOPOSTGRES")
    .source(sourceType = "relational", sourceFormat = "hive")
    .target(targetType = "relational", targetFormat = "jdbc")
    .getConnector

hiveJdbcConnector.setVeboseLogging(true)      

hiveJdbcConnector
    .source("persons")
    .saveAs("__hive_staging__")
    .transformSql("""select 
      |    id,
      |    cast (name as STRING) as name,
      |    cast (ownerid as STRING) as ownerid,
      |    age__c as age,
      |    cast (gender__c as STRING) as gender 
      |    from __hive_staging___""".stripMargin)
    .targetJDBC(objectName="test",props=properties,saveMode=SaveMode.Overwrite)
hiveJdbcConnector.stop()    

```

### NOSQL Source

![image](https://user-images.githubusercontent.com/59328701/120105831-40a93780-c178-11eb-842e-08b8e6a57e40.png)


#### MongoDB to Postgres Connector
The sourceObject in case of mongo is nothing but the collection name person in db spear.The format for source object is <db_name>.<collection_name>

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

val targetProps = Map(
  "driver" -> "org.postgresql.Driver",
  "user" -> "postgres_user",
  "password" -> "mysecretpassword",
  "url" -> "jdbc:postgresql://postgres:5432/pgdb"
)

Logger.getLogger("com.github").setLevel(Level.INFO)  

val mongoToPostgresConnector = SpearConnector
    .createConnector("Mongo-Postgres")
    .source(sourceType = "nosql", sourceFormat = "mongo")
    .target(targetType = "relational", targetFormat = "jdbc")
    .getConnector

mongoToPostgresConnector.setVeboseLogging(true)

mongoToPostgresConnector
    .source(sourceObject = "spear.person",Map("uri" -> "mongodb://mongo:27017"))
    .saveAs("_mongo_staging_")
    .transformSql(
      """
        |select cast(id as INT) as person_id,
        |cast (name as STRING) as person_name,
        |cast (sal as FLOAT) as salary
        |from _mongo_staging_ """.stripMargin)
    .targetJDBC(objectName="mongo_data", props=targetProps, saveMode=SaveMode.Overwrite)

mongoToPostgresConnector.stop()    
```

### Output

```commandline
21/05/29 14:29:09 INFO targetjdbc.NOSQLtoJDBC: Connector:Mongo-Postgres to Target: JDBC with Format: jdbc from Source Object: spear.person with Format: mongo started running!!
21/05/29 14:29:09 INFO targetjdbc.NOSQLtoJDBC: Reading source object:spear.person with format: mongo status:success
+--------------------------+---+------+------+
|_id                       |id |name  |sal   |
+--------------------------+---+------+------+
|[60b219a429ff3ceddd1dd59d]|1.0|Anu   |5000.0|
|[60b219f729ff3ceddd1dd59e]|2.0|Deep  |6000.0|
|[60b2379429ff3ceddd1dd59f]|3.0|Rami  |6000.0|
|[60b237e629ff3ceddd1dd5a0]|4.0|Raj   |6000.0|
|[60b2381d29ff3ceddd1dd5a1]|6.0|Rahul |6000.0|
|[60b2381d29ff3ceddd1dd5a2]|5.0|Ravi  |7000.0|
|[60b2383d29ff3ceddd1dd5a3]|7.0|Kalyan|8000.0|
|[60b2383d29ff3ceddd1dd5a4]|8.0|Rajesh|9000.0|
|[60b2385e29ff3ceddd1dd5a5]|8.0|Nav   |6000.0|
|[60b2385e29ff3ceddd1dd5a6]|9.0|Ravi  |7000.0|
+--------------------------+---+------+------+

21/05/29 14:29:10 INFO targetjdbc.NOSQLtoJDBC: Saving data as temporary table:_mongo_staging_ success
21/05/29 14:29:10 INFO targetjdbc.NOSQLtoJDBC: Executing tranformation sql:
select cast(id as INT) as person_id,
cast (name as STRING) as person_name,
cast (sal as FLOAT) as salary
from _mongo_staging_  status :success
+---------+-----------+------+
|person_id|person_name|salary|
+---------+-----------+------+
|1        |Anu        |5000.0|
|2        |Deep       |6000.0|
|3        |Rami       |6000.0|
|4        |Raj        |6000.0|
|6        |Rahul      |6000.0|
|5        |Ravi       |7000.0|
|7        |Kalyan     |8000.0|
|8        |Rajesh     |9000.0|
|8        |Nav        |6000.0|
|9        |Ravi       |7000.0|
+---------+-----------+------+

21/05/29 14:29:10 INFO targetjdbc.NOSQLtoJDBC: Write data to table/object:mongo_data completed with status:success
+---------+-----------+------+
|person_id|person_name|salary|
+---------+-----------+------+
|1        |Anu        |5000.0|
|2        |Deep       |6000.0|
|3        |Rami       |6000.0|
|4        |Raj        |6000.0|
|6        |Rahul      |6000.0|
|5        |Ravi       |7000.0|
|7        |Kalyan     |8000.0|
|8        |Rajesh     |9000.0|
|8        |Nav        |6000.0|
|9        |Ravi       |7000.0|
+---------+-----------+------+
```

### Streaming source

![image](https://user-images.githubusercontent.com/59328701/119257031-1cca7c80-bbe1-11eb-94ff-9b135bac11b5.png)


#### Kafka to Postgres Connector

For real -time streaming form kafka/twitter etc.., a schema must be necessarily provided for seamlesa streaming.

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val targetProps = Map(
  "driver" -> "org.postgresql.Driver",
  "user" -> "postgres_user",
  "password" -> "mysecretpassword",
  "url" -> "jdbc:postgresql://postgres:5432/pgdb"
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
    .targetJDBC(objectName="person", props=properties, saveMode=SaveMode.Append)

streamTOPostgres.stop()
```

### GraphDB Source
![image](https://user-images.githubusercontent.com/59328701/122394272-6b2a2b80-cf93-11eb-8f0b-c51e91f71726.png)

#### Neo4j to Postgres Connector
```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)
val targetParams = Map(
  "driver" -> "org.postgresql.Driver",
  "user" -> "postgres_user",
  "password" -> "mysecretpassword",
  "url" -> "jdbc:postgresql://postgres:5432/pgdb"
)

val neoPostgresConnector = SpearConnector
  .createConnector("Neo4jTOPostgres")
  .source(sourceType = "graph", sourceFormat = "neo4j")
  .target(targetType = "relational", targetFormat = "jdbc")
  .getConnector

neoPostgresConnector.setVeboseLogging(true)

neoPostgresConnector
  .source(sourceObject = "elections", Map("url"-> "bolt://host:7687",
    "authentication.basic.username"-> "neo4j",
    "authentication.basic.password"-> "****",
    "labels"-> "elections"))
  .saveAs("__tmp__")
  .transformSql(
    """select state_code,party,
      |sum(votes) as total_votes
      |from __tmp__
      |group by state_code,party""".stripMargin)
  .targetJDBC(objectName = "elections", props = targetParams, saveMode = SaveMode.Overwrite)
neoPostgresConnector.stop()   
```

##### Output

```commandline
21/06/17 12:00:32 INFO targetjdbc.GraphtoJDBC: Connector:Neo4jTOPostgres to Target:JDBC with Format:jdbc from GraphDB Object:elections with Format:neo4j started running!!
21/06/17 12:00:32 INFO targetjdbc.GraphtoJDBC: Reading source table:elections with format:neo4j status:success
+----+-----------+-----+----------+-----+---------+----------+-------------------+------------+----------+
|<id>|<labels>   |votes|state_code|party|last_name|first_name|country_total_votes|country_name|country_id|
+----+-----------+-----+----------+-----+---------+----------+-------------------+------------+----------+
|10  |[elections]|91696|AK        |Dem  |Obama    |Barack    |220596             |Alasaba     |1         |
|11  |[elections]|91696|AK        |Dem  |Obama    |Barack    |220596             |Akaskak     |2         |
|12  |[elections]|6354 |AL        |Dem  |Obama    |Barack    |23909              |Autauga     |3         |
|13  |[elections]|91696|AK        |Dem  |Obama    |Barack    |220596             |Akaska      |4         |
|14  |[elections]|18329|AL        |Dem  |Obama    |Barack    |84988              |Baldwin     |5         |
|15  |[elections]|5873 |AL        |Dem  |Obama    |Barack    |11459              |Barbour     |6         |
|16  |[elections]|2200 |AL        |Dem  |Obama    |Barack    |8391               |Bibb        |7         |
|17  |[elections]|2961 |AL        |Dem  |Obama    |Barack    |23980              |Blount      |8         |
|18  |[elections]|4058 |AL        |Dem  |Obama    |Barack    |5318               |Bullock     |9         |
|19  |[elections]|4367 |AL        |Dem  |Obama    |Barack    |9483               |Butler      |10        |
+----+-----------+-----+----------+-----+---------+----------+-------------------+------------+----------+
only showing top 10 rows

21/06/17 12:00:33 INFO targetjdbc.GraphtoJDBC: Saving data as temporary table:__tmp__ success
21/06/17 12:00:33 INFO targetjdbc.GraphtoJDBC: Executing transformation sql: select state_code,party,
sum(votes) as total_votes
from __tmp__
group by state_code,party status :success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AK        |Dem  |275088     |
|AL        |Dem  |793620     |
|AR        |Dem  |391953     |
|AZ        |Dem  |900081     |
|CA        |Dem  |6241648    |
|CO        |CST  |5807       |
|CT        |GOP  |631432     |
|DC        |Dem  |222332     |
|DE        |Dem  |242547     |
|FL        |GOP  |4162081    |
+----------+-----+-----------+
only showing top 10 rows

21/06/17 12:00:35 INFO targetjdbc.GraphtoJDBC: Write data to table/object:elections completed with status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AK        |Dem  |275088     |
|AL        |Dem  |793620     |
|AR        |Dem  |391953     |
|AZ        |Dem  |900081     |
|CA        |Dem  |6241648    |
|CO        |CST  |5807       |
|CT        |GOP  |631432     |
|DC        |Dem  |222332     |
|DE        |Dem  |242547     |
|FL        |GOP  |4162081    |
+----------+-----+-----------+
only showing top 10 rows

```



## Target FS (HDFS)

Using Spear-framework you an write connectors to File Systems both hadoop(HDFS) or any cloud file system(S3/GCS/adls etc).For HDFS as file system spark will create a table in hive and for that the database specified must exist in hive .Spark will not create the database,it just creates a table within that database. 

Points to be remembered:
1. While writing to hdfs using spear ,the destination file path is not mandatory.If specified an external table will be created on top of the specified path or else a managed table will be created in hive.
2. Optional additional properties for any FS(s3/HDFS/GCS/ADLS) target can have the following as target properties:

| sno   | param             |  description                                                                               |
| ------|:-----------------:| ------------------------------------------------------------------------------------------:|
| 1     | partition_columns | list of comma seperated col names for partitions Ex: Map("partition_columns"->"col1,col2") |
| 2     | num_buckets       | no: of buckets to create.Used mostly with HDFS as FS Ex: Map("num_buckets"->"4")           |
| 3     | bucket_columns    | list of comma seperated col names for buckets.                                             |


![image](https://user-images.githubusercontent.com/59328701/120105944-a0074780-c178-11eb-85bf-877876795e86.png)

### JDBC source

#### Postgres to Hive Connector
This connector is used to move data from postgres to hive in a partitioned table

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}

Logger.getLogger("com.github").setLevel(Level.INFO)
val postgresToHiveConnector = SpearConnector
  .createConnector(name = "PostgrestoHiveConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .getConnector

postgresToHiveConnector.setVeboseLogging(true)

val hivePartitionParams = Map(
  "partition_columns" -> "state_code"
)
postgresToHiveConnector
  .source("mytable", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("__tmp__")
  .transformSql(
    """select state_code,party,
      |total_votes
      |from __tmp__""".stripMargin)
  .targetFS(destinationFilePath = "/user/hive/metastore/ingest_test.db", props = hivePartitionParams, saveAsTable = "ingest_test.postgres_data", saveMode = SaveMode.Overwrite)
  
postgresToHiveConnector.stop()
```

### Output

```commandline

21/06/17 10:30:03 INFO targetFS.JDBCtoFS: Connector:PostgrestoHiveConnector to Target:File System with Format:parquet from Source table/Object:mytable with Format:jdbc started running!!
21/06/17 10:30:03 INFO targetFS.JDBCtoFS: Reading source table: mytable with format: jdbc status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|AL        |Dem  |793620     |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

21/06/17 10:30:03 INFO targetFS.JDBCtoFS: Saving data as temporary table:__tmp__ success
21/06/17 10:30:03 INFO targetFS.JDBCtoFS: Executing transformation sql: select state_code,party,
total_votes
from __tmp__ status :success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|AL        |Dem  |793620     |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

21/06/17 10:30:20 INFO targetFS.JDBCtoFS: Write data to target path:/tmp/ingest_test.db with format:parquet and saved as table:ingest_test.postgres_data completed with status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|AL        |Dem  |793620     |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|MN        |Grn  |13045      |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows

Data in hdfs:
[root@hadoop /]# hdfs dfs -ls /user/hive/metastore/ingest_test.db/postgres_data
21/06/17 11:05:48 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 52 items
-rw-r--r--   1 root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/_SUCCESS
drwxr-xr-x   - root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/state_code=AK
drwxr-xr-x   - root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/state_code=AL
drwxr-xr-x   - root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/state_code=AR
drwxr-xr-x   - root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/state_code=AZ
drwxr-xr-x   - root supergroup          0 2021-06-17 11:05 /user/hive/metastore/ingest_test.db/postgres_data/state_code=CA
```

#### Oracle to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val oraToHiveConnector = SpearConnector
  .createConnector(name = "OracleTotoHiveConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .getConnector

oraToHiveConnector.setVeboseLogging(true)
oraToHiveConnector
.sourceSql(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "user" -> "user", "password" -> "pass", "url" -> "jdbc:oracle:thin:@ora-host:1521:orcl"),
  """
    |SELECT
    |        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
    |        cast(NUMBER_TYPE as varchar(255)) as number_type,
    |        cast(TINYINT_TYPE_1 as varchar(255)) as tinyint_type_1,
    |        cast(TINYINT_TYPE_2 as varchar(255)) as tinyint_type_2,
    |        cast(INT_TYPE_5 as varchar(255)) as int_type_5,
    |        cast(INT_TYPE_9 as varchar(255)) as int_type_9,
    |        cast(BIGINT_TYPE_15 as varchar(255)) as bigint_type_15,
    |        cast(BIGINT_TYPE_18 as varchar(255)) as bigint_type_18,
    |        cast(NUMBER_TYPE_31 as varchar(255)) as number_type_31,
    |        cast(NUMBER_TYPE_38 as varchar(255)) as number_type_38,
    |        cast(NUMBER_TYPE_7_4 as varchar(255)) as number_type_7_4,
    |        cast(NUMBER_TYPE_13_7 as varchar(255)) as number_type_13_7
    |        from DBSRV.ORACLE_NUMBER
    |""".stripMargin
).saveAs("__TF_SOURCE_TABLE__")
  .transformSql(
    """
      |SELECT
      |        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
      |        NUMBER_TYPE as number_type,
      |        cast(TINYINT_TYPE_1 as TINYINT) as tinyint_type_1,
      |        cast(TINYINT_TYPE_2 as TINYINT) as tinyint_type_2,
      |        cast(INT_TYPE_5 as INT) as int_type_5,
      |        cast(INT_TYPE_9 as INT) as int_type_9,
      |        cast(BIGINT_TYPE_15 as BIGINT) as bigint_type_15,
      |        cast(BIGINT_TYPE_18 as BIGINT) as bigint_type_18,
      |        cast(NUMBER_TYPE_31 as DECIMAL(31,0)) as number_type_31,
      |        cast(NUMBER_TYPE_38 as DECIMAL(38,0)) as number_type_38,
      |        cast(NUMBER_TYPE_7_4 as DECIMAL(7,4)) as number_type_7_4,
      |        cast(NUMBER_TYPE_13_7 as DECIMAL(13,7)) as number_type_13_7
      |        from __TF_SOURCE_TABLE__
      |""".stripMargin)
  .targetFS(destinationFilePath = "/tmp/ingest_test.db", saveAsTable = "ingest_test.ora_data", saveMode=SaveMode.Overwrite)
oraToHiveConnector.stop()

```

### Output

```commandline
21/05/03 17:19:46 INFO targetFS.JDBCtoFS:Connector:OracleTotoHiveConnector from Source sql:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        cast(NUMBER_TYPE as varchar(255)) as number_type,
        cast(TINYINT_TYPE_1 as varchar(255)) as tinyint_type_1,
        cast(TINYINT_TYPE_2 as varchar(255)) as tinyint_type_2,
        cast(INT_TYPE_5 as varchar(255)) as int_type_5,
        cast(INT_TYPE_9 as varchar(255)) as int_type_9,
        cast(BIGINT_TYPE_15 as varchar(255)) as bigint_type_15,
        cast(BIGINT_TYPE_18 as varchar(255)) as bigint_type_18,
        cast(NUMBER_TYPE_31 as varchar(255)) as number_type_31,
        cast(NUMBER_TYPE_38 as varchar(255)) as number_type_38,
        cast(NUMBER_TYPE_7_4 as varchar(255)) as number_type_7_4,
        cast(NUMBER_TYPE_13_7 as varchar(255)) as number_type_13_7
        from DBSRV.ORACLE_NUMBER
 with Format: jdbc started running!!
21/05/03 17:19:46 INFO targetFS.JDBCtoFS: Executing source sql query:
SELECT
        to_char(sys_extract_utc(systimestamp), 'YYYY-MM-DD HH24:MI:SS.FF') as ingest_ts_utc,
        cast(NUMBER_TYPE as varchar(255)) as number_type,
        cast(TINYINT_TYPE_1 as varchar(255)) as tinyint_type_1,
        cast(TINYINT_TYPE_2 as varchar(255)) as tinyint_type_2,
        cast(INT_TYPE_5 as varchar(255)) as int_type_5,
        cast(INT_TYPE_9 as varchar(255)) as int_type_9,
        cast(BIGINT_TYPE_15 as varchar(255)) as bigint_type_15,
        cast(BIGINT_TYPE_18 as varchar(255)) as bigint_type_18,
        cast(NUMBER_TYPE_31 as varchar(255)) as number_type_31,
        cast(NUMBER_TYPE_38 as varchar(255)) as number_type_38,
        cast(NUMBER_TYPE_7_4 as varchar(255)) as number_type_7_4,
        cast(NUMBER_TYPE_13_7 as varchar(255)) as number_type_13_7
        from DBSRV.ORACLE_NUMBER

21/05/03 17:19:46 INFO targetFS.JDBCtoFS: Data is saved as a temporary table by name: __TF_SOURCE_TABLE__
21/05/03 17:19:46 INFO targetFS.JDBCtoFS: showing saved data from temporary table with name: __TF_SOURCE_TABLE__
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|INGEST_TS_UTC             |NUMBER_TYPE       |TINYINT_TYPE_1|TINYINT_TYPE_2|INT_TYPE_5|INT_TYPE_9|BIGINT_TYPE_15 |BIGINT_TYPE_18    |NUMBER_TYPE_31             |NUMBER_TYPE_38                 |NUMBER_TYPE_7_4|NUMBER_TYPE_13_7|
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|2021-05-03 17:19:46.664410|1244124343.2341111|5             |89            |98984     |788288722 |788288722989848|788288722989848897|788288722989848897788288722|7882887229898488977882887228987|322.1311       |132431.2144     |
|2021-05-03 17:19:46.664410|78441243.2341111  |9             |89            |98984     |7888722   |788722989848   |288722989848897   |7882887229898488288722     |78828872288977882887228987     |322.1311       |132431.2144     |
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+

21/05/03 17:19:46 INFO targetFS.JDBCtoFS: Data after transformation using the SQL :
SELECT
        TO_TIMESTAMP(ingest_ts_utc) as ingest_ts_utc,
        NUMBER_TYPE as number_type,
        cast(TINYINT_TYPE_1 as TINYINT) as tinyint_type_1,
        cast(TINYINT_TYPE_2 as TINYINT) as tinyint_type_2,
        cast(INT_TYPE_5 as INT) as int_type_5,
        cast(INT_TYPE_9 as INT) as int_type_9,
        cast(BIGINT_TYPE_15 as BIGINT) as bigint_type_15,
        cast(BIGINT_TYPE_18 as BIGINT) as bigint_type_18,
        cast(NUMBER_TYPE_31 as DECIMAL(31,0)) as number_type_31,
        cast(NUMBER_TYPE_38 as DECIMAL(38,0)) as number_type_38,
        cast(NUMBER_TYPE_7_4 as DECIMAL(7,4)) as number_type_7_4,
        cast(NUMBER_TYPE_13_7 as DECIMAL(13,7)) as number_type_13_7
        from __TF_SOURCE_TABLE__

+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|ingest_ts_utc             |number_type       |tinyint_type_1|tinyint_type_2|int_type_5|int_type_9|bigint_type_15 |bigint_type_18    |number_type_31             |number_type_38                 |number_type_7_4|number_type_13_7|
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|2021-05-03 17:19:46.973852|1244124343.2341111|5             |89            |98984     |788288722 |788288722989848|788288722989848897|788288722989848897788288722|7882887229898488977882887228987|322.1311       |132431.2144000  |
|2021-05-03 17:19:46.973852|78441243.2341111  |9             |89            |98984     |7888722   |788722989848   |288722989848897   |7882887229898488288722     |78828872288977882887228987     |322.1311       |132431.2144000  |
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+

21/05/03 17:19:46 INFO targetFS.JDBCtoFS: Writing data to target file: /tmp/ingest_test.db
21/05/03 17:19:53 INFO targetFS.JDBCtoFS: Saving data to table:ingest_test.ora_data
21/05/03 17:19:53 INFO targetFS.JDBCtoFS: Target Data in table:ingest_test.ora_data

+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|ingest_ts_utc             |number_type       |tinyint_type_1|tinyint_type_2|int_type_5|int_type_9|bigint_type_15 |bigint_type_18    |number_type_31             |number_type_38                 |number_type_7_4|number_type_13_7|
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+
|2021-05-03 17:19:47.533503|1244124343.2341111|5             |89            |98984     |788288722 |788288722989848|788288722989848897|788288722989848897788288722|7882887229898488977882887228987|322.1311       |132431.2144000  |
|2021-05-03 17:19:47.533503|78441243.2341111  |9             |89            |98984     |7888722   |788722989848   |288722989848897   |7882887229898488288722     |78828872288977882887228987     |322.1311       |132431.2144000  |
+--------------------------+------------------+--------------+--------------+----------+----------+---------------+------------------+---------------------------+-------------------------------+---------------+----------------+

```

#### Salesforce to Hive Connector
Spark cannot read a salesforce object directly if specified,You must always use a soql or a saql query to read from the Salesforce object.So we need to use sourceSql API.

```scala

  import com.github.edge.roman.spear.SpearConnector
  import org.apache.spark.sql.{Column, DataFrame, SaveMode}
  val salseforceToHiveConnector = SpearConnector
    .createConnector("salseforce-hive")
    .source(sourceType = "relational", sourceFormat = "soql")
    .target(targetType = "FS", targetFormat = "parquet")
    .getConnector

  salseforceToHiveConnector.sourceSql(Map( "login"-> "https://login.salesforce.com","username" -> "user", "password" -> "pass"),
    """select
          |    Id,
          |    name,
          |    OwnerId,
          |    age__c,
          |    gender__c,
          |    guardianname__c,
          |    ingest_ts_utc__c,
          |    name_siml_score__c,
          |    year_of_birth__c,
          |    LastModifiedById,
          |    LastModifiedDate
          |    from sales_test__c""".stripMargin).saveAs("__temp__")
    .transformSql("""select
      |    Id,
      |    name,
      |    OwnerId,
      |    age__c,
      |    gender__c,
      |    guardianname__c,
      |    ingest_ts_utc__c,
      |    cast(name_siml_score__c as Float) name_siml_score__c,
      |    year_of_birth__c,
      |    LastModifiedById,
      |    cast(unix_timestamp(LastModifiedDate,"yyyy-MM-dd") AS timestamp) as LastModifiedDate
      |    from __temp__""".stripMargin)
    .targetFS(destinationFilePath="/user/hive/metastore/salseforce.db",saveAsTable="salseforce.transform_hive",saveMode=SaveMode.Overwrite)
    
    salseforceToHiveConnector.stop()
```
### Output

```commandline
21/05/29 08:08:15 INFO targetFS.JDBCtoFS: Connector:salseforce-hive from Source sql: 
select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with Format: soql started running!!
21/05/29 08:08:37 INFO targetFS.JDBCtoFS: Executing source sql query: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with format: soql status:success
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|LastModifiedDate            |OwnerId           |age__c|gender__c|name_siml_score__c|guardianname__c|year_of_birth__c|Id                |LastModifiedById  |ingest_ts_utc__c     |Name        |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.583333          |prahlad ram    |1962.0          |a045g000001RNLnAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|magi devi   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |1.0               |kisan lal      |1959.0          |a045g000001RNLoAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|bhanwar lal |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |bhagwan singh  |1967.0          |a045g000001RNLpAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|chain kanvar|
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |null              |pepa ram       |1998.0          |a045g000001RNLqAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|baga ram    |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.333333          |sataram        |1974.0          |a045g000001RNLrAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|prabhu ram  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.357143          |jiyaram        |1981.0          |a045g000001RNLsAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|karna ram   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |hameera ram    |1992.0          |a045g000001RNLtAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|vimla       |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.6               |shyam singh    |1987.0          |a045g000001RNLuAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|brij kanvar |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |viramaram      |1936.0          |a045g000001RNLvAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|javarodevi  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.583333          |bagataram      |1981.0          |a045g000001RNLwAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|arjun ram   |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
only showing top 10 rows

21/05/29 08:08:37 INFO targetFS.JDBCtoFS: Saving data as temporary table:__temp__ success
21/05/29 08:08:37 INFO targetFS.JDBCtoFS: Executing tranformation sql: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    cast(name_siml_score__c as Float) name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    cast(unix_timestamp(LastModifiedDate,"yyyy-MM-dd") AS timestamp) as LastModifiedDate
    from __temp__ status :success
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|Id                |name        |OwnerId           |age__c|gender__c|guardianname__c|ingest_ts_utc__c     |name_siml_score__c|year_of_birth__c|LastModifiedById  |LastModifiedDate   |
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|a045g000001RNLnAAO|magi devi   |0055g0000043QwfAAE|null  |F        |prahlad ram    |2020-05-20 16:01:25.0|0.583333          |1962.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLoAAO|bhanwar lal |0055g0000043QwfAAE|null  |M        |kisan lal      |2020-05-20 16:01:25.0|1.0               |1959.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLpAAO|chain kanvar|0055g0000043QwfAAE|null  |F        |bhagwan singh  |2020-05-20 16:01:25.0|1.0               |1967.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLqAAO|baga ram    |0055g0000043QwfAAE|null  |M        |pepa ram       |2020-05-20 16:01:25.0|null              |1998.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLrAAO|prabhu ram  |0055g0000043QwfAAE|null  |M        |sataram        |2020-05-20 16:01:25.0|0.333333          |1974.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLsAAO|karna ram   |0055g0000043QwfAAE|null  |M        |jiyaram        |2020-05-20 16:01:25.0|0.357143          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLtAAO|vimla       |0055g0000043QwfAAE|null  |F        |hameera ram    |2020-05-20 16:01:25.0|1.0               |1992.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLuAAO|brij kanvar |0055g0000043QwfAAE|null  |F        |shyam singh    |2020-05-20 16:01:25.0|0.6               |1987.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLvAAO|javarodevi  |0055g0000043QwfAAE|null  |F        |viramaram      |2020-05-20 16:01:25.0|1.0               |1936.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLwAAO|arjun ram   |0055g0000043QwfAAE|null  |M        |bagataram      |2020-05-20 16:01:25.0|0.583333          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
only showing top 10 rows

21/05/29 08:08:39 INFO targetFS.JDBCtoFS: Write data to target path:/tmp/salesforce with format: soql and saved as table:salesforcehive completed with status:success
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|Id                |name        |OwnerId           |age__c|gender__c|guardianname__c|ingest_ts_utc__c     |name_siml_score__c|year_of_birth__c|LastModifiedById  |LastModifiedDate   |
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|a045g000001RNLnAAO|magi devi   |0055g0000043QwfAAE|null  |F        |prahlad ram    |2020-05-20 16:01:25.0|0.583333          |1962.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLoAAO|bhanwar lal |0055g0000043QwfAAE|null  |M        |kisan lal      |2020-05-20 16:01:25.0|1.0               |1959.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLpAAO|chain kanvar|0055g0000043QwfAAE|null  |F        |bhagwan singh  |2020-05-20 16:01:25.0|1.0               |1967.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLqAAO|baga ram    |0055g0000043QwfAAE|null  |M        |pepa ram       |2020-05-20 16:01:25.0|null              |1998.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLrAAO|prabhu ram  |0055g0000043QwfAAE|null  |M        |sataram        |2020-05-20 16:01:25.0|0.333333          |1974.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLsAAO|karna ram   |0055g0000043QwfAAE|null  |M        |jiyaram        |2020-05-20 16:01:25.0|0.357143          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLtAAO|vimla       |0055g0000043QwfAAE|null  |F        |hameera ram    |2020-05-20 16:01:25.0|1.0               |1992.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLuAAO|brij kanvar |0055g0000043QwfAAE|null  |F        |shyam singh    |2020-05-20 16:01:25.0|0.6               |1987.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLvAAO|javarodevi  |0055g0000043QwfAAE|null  |F        |viramaram      |2020-05-20 16:01:25.0|1.0               |1936.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLwAAO|arjun ram   |0055g0000043QwfAAE|null  |M        |bagataram      |2020-05-20 16:01:25.0|0.583333          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
only showing top 10 rows
```

### NOSQL Source

![image](https://user-images.githubusercontent.com/59328701/120105977-d9d84e00-c178-11eb-91aa-6dc7c83aa777.png)

#### MongoDB to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

Logger.getLogger("com.github").setLevel(Level.INFO)  
val mongoToHiveConnector = SpearConnector
    .createConnector("Mongo-Hive")
    .source(sourceType = "nosql", sourceFormat = "mongo")
    .target(targetType = "FS", targetFormat = "parquet")
    .getConnector
mongoToHiveConnector.setVeboseLogging(true)
mongoToHiveConnector
    .source(sourceObject = "spear.person",Map("uri" -> "mongodb://mongo:27017"))
    .saveAs("_mongo_staging_")
    .transformSql(
      """
        |select cast(id as INT) as person_id,
        |cast (name as STRING) as person_name,
        |cast (sal as FLOAT) as salary
        |from _mongo_staging_ """.stripMargin)
    .targetFS(destinationFilePath = "/tmp/mongo.db",saveAsTable="mongohive" , saveMode=SaveMode.Overwrite) 

mongoToHiveConnector.stop()    
```

### Streaming source

![image](https://user-images.githubusercontent.com/59328701/119257185-cf9ada80-bbe1-11eb-91c6-bef2d21da739.png)


#### Kafka to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val streamTOHdfs=SpearConnector
  .createConnector(name="StreamKafkaToHiveconnector")
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

## Cloud source

#### S3 to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "*****")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******")

 val s3ToHiveConnector = SpearConnector
    .createConnector("salseforce-hive")
    .source(sourceType = "FS", sourceFormat = "parquet")
    .target(targetType = "FS", targetFormat = "parquet")
    .getConnector
 s3ToHiveConnector
 .source("s3a://testbucketspear/salesforcedata")
.targetFS(destinationFilePath = "",saveAsTable="ingest_test.salesforce",saveMode=SaveMode.Overwrite)

s3ToHiveConnector.stop()
```

### Output

```commandline
21/05/29 08:02:18 INFO targetFS.FiletoFS: Connector to Target: File System with Format: parquet from Source: s3a://testbucketspear/salesforcedata with Format: s3a://testbucketspear/salesforcedata started running !!
21/05/29 08:02:19 INFO targetFS.FiletoFS: Reading source file: s3a://testbucketspear/salesforcedata with format: parquet status:success
21/05/29 08:02:21 INFO targetFS.FiletoFS: Write data to target path:  with format: parquet and saved as table ingest_test.salesforce completed with status:success
```


## Target FS (Cloud)
We can also write to Cloud File Systems using Spear. Below are few of the connectors to Cloud from different sources.

![image](https://user-images.githubusercontent.com/59328701/120101557-2ebd9980-c164-11eb-95b6-1925bd68d673.png)


### JDBC source

#### Oracle To S3 Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "*****")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "*****")


val oracleTOS3Connector = SpearConnector.init
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .target(targetType = "FS", targetFormat = "parquet")
  .withName(connectorName ="OracleToS3Connector" )
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
  .targetFS(destinationFilePath="s3a://destination/data",saveMode=SaveMode.Overwrite)

oracleTOS3Connector.stop()
```

### Output

```commandline
21/05/03 17:19:46 INFO targetFS.JDBCtoFS:Connector:OracleTotoHiveConnector from Source sql:
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
 with Format: jdbc started running!!
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

#### Postgres To GCS Connector

```scala

import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

spark.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile","*********")

Logger.getLogger("com.github").setLevel(Level.INFO)
val postgresToGCSConnector = SpearConnector
    .createConnector("PostgresToGCSConnector")
    .source(sourceType = "relational", sourceFormat = "jdbc")
    .target(targetType = "FS", targetFormat = "parquet")
    .getConnector
postgresToGCSConnector.setVeboseLogging(true)
postgresToGCSConnector
    .source("mytable", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
    .saveAs("__tmp__")
    .transformSql(
      """
        |select *
        |from __tmp__""".stripMargin)
    .targetFS(destinationFilePath = "gs://testbucketspear/ora_data", saveMode=SaveMode.Overwrite)

postgresToGCSConnector.stop()
```

### Output
```commandline
21/05/29 08:27:33 INFO targetFS.JDBCtoFS: Connector:PostgresToGCSConnector to Target:File System with Format:parquet from Source Object:mytable with Format:jdbc started running!!
21/05/29 08:27:33 INFO targetFS.JDBCtoFS: Reading source table:mytable with format:jdbc status:success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|MN        |Grn  |13045      |
|NY        |GOP  |2226637    |
|AZ        |Lib  |27523      |
|MI        |CST  |16792      |
|ID        |Dem  |212560     |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
+----------+-----+-----------+
only showing top 10 rows

21/05/29 08:27:33 INFO targetFS.JDBCtoFS: Saving data as temporary table:__tmp__ success
21/05/29 08:27:33 INFO targetFS.JDBCtoFS: Executing tranformation sql:
select *
from __tmp__ status :success
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|MN        |Grn  |13045      |
|NY        |GOP  |2226637    |
|AZ        |Lib  |27523      |
|MI        |CST  |16792      |
|ID        |Dem  |212560     |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
+----------+-----+-----------+
only showing top 10 rows

21/05/29 08:27:51 INFO targetFS.JDBCtoFS: Write data to target path:gs://testbucketspear/mytable_data with format:parquet completed with status:success
```


#### Salesforce to S3 Connector

```
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "*******")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "*******")

Logger.getLogger("com.github").setLevel(Level.INFO)
val salseforceToS3Connector = SpearConnector
    .createConnector("salesforce-S3")
    .source(sourceType = "relational", sourceFormat = "soql")
    .target(targetType = "FS", targetFormat = "parquet")
    .getConnector

salseforceToS3Connector.setVeboseLogging(true)
salseforceToS3Connector
   .sourceSql(Map( "login"-> "https://login.salesforce.com","username" -> "******", "password" -> "****"),
    """select
      |    Id,
      |    name,
      |    OwnerId,
      |    age__c,
      |    gender__c,
      |    guardianname__c,
      |    ingest_ts_utc__c,
      |    name_siml_score__c,
      |    year_of_birth__c,
      |    LastModifiedById,
      |    LastModifiedDate
      |    from sale_test__c""".stripMargin)
    .saveAs("__temp__")
    .transformSql("""select
      |    Id,
      |    name,
      |    OwnerId,
      |    age__c,
      |    gender__c,
      |    guardianname__c,
      |    ingest_ts_utc__c,
      |    cast(name_siml_score__c as Float) name_siml_score__c,
      |    year_of_birth__c,
      |    LastModifiedById,
      |    cast(unix_timestamp(LastModifiedDate,"yyyy-MM-dd") AS timestamp) as LastModifiedDate
      |    from __temp__""".stripMargin)
    .targetFS(destinationFilePath = "s3a://testbucketspear/salesforcedata", saveMode=SaveMode.Overwrite)
    
salseforceToS3Connector.stop()
```

### Output

```commandline
21/05/29 07:08:03 INFO targetFS.JDBCtoFS: Connector:salesforce-S3 from Source sql: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with Format: soql started running!!
21/05/29 07:08:26 INFO targetFS.JDBCtoFS: Executing source sql query: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with format: soql status:success
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|LastModifiedDate            |OwnerId           |age__c|gender__c|name_siml_score__c|guardianname__c|year_of_birth__c|Id                |LastModifiedById  |ingest_ts_utc__c     |Name        |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.583333          |prahlad ram    |1962.0          |a045g000001RNLnAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|magi devi   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |1.0               |kisan lal      |1959.0          |a045g000001RNLoAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|bhanwar lal |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |bhagwan singh  |1967.0          |a045g000001RNLpAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|chain kanvar|
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |null              |pepa ram       |1998.0          |a045g000001RNLqAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|baga ram    |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.333333          |sataram        |1974.0          |a045g000001RNLrAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|prabhu ram  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.357143          |jiyaram        |1981.0          |a045g000001RNLsAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|karna ram   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |hameera ram    |1992.0          |a045g000001RNLtAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|vimla       |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.6               |shyam singh    |1987.0          |a045g000001RNLuAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|brij kanvar |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |viramaram      |1936.0          |a045g000001RNLvAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|javarodevi  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.583333          |bagataram      |1981.0          |a045g000001RNLwAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|arjun ram   |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
only showing top 10 rows

21/05/29 07:08:26 INFO targetFS.JDBCtoFS: Saving data as temporary table:__temp__ success
21/05/29 07:08:26 INFO targetFS.JDBCtoFS: Executing tranformation sql: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    cast(name_siml_score__c as Float) name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    cast(unix_timestamp(LastModifiedDate,"yyyy-MM-dd") AS timestamp) as LastModifiedDate
    from __temp__ status :success
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|Id                |name        |OwnerId           |age__c|gender__c|guardianname__c|ingest_ts_utc__c     |name_siml_score__c|year_of_birth__c|LastModifiedById  |LastModifiedDate   |
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
|a045g000001RNLnAAO|magi devi   |0055g0000043QwfAAE|null  |F        |prahlad ram    |2020-05-20 16:01:25.0|0.583333          |1962.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLoAAO|bhanwar lal |0055g0000043QwfAAE|null  |M        |kisan lal      |2020-05-20 16:01:25.0|1.0               |1959.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLpAAO|chain kanvar|0055g0000043QwfAAE|null  |F        |bhagwan singh  |2020-05-20 16:01:25.0|1.0               |1967.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLqAAO|baga ram    |0055g0000043QwfAAE|null  |M        |pepa ram       |2020-05-20 16:01:25.0|null              |1998.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLrAAO|prabhu ram  |0055g0000043QwfAAE|null  |M        |sataram        |2020-05-20 16:01:25.0|0.333333          |1974.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLsAAO|karna ram   |0055g0000043QwfAAE|null  |M        |jiyaram        |2020-05-20 16:01:25.0|0.357143          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLtAAO|vimla       |0055g0000043QwfAAE|null  |F        |hameera ram    |2020-05-20 16:01:25.0|1.0               |1992.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLuAAO|brij kanvar |0055g0000043QwfAAE|null  |F        |shyam singh    |2020-05-20 16:01:25.0|0.6               |1987.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLvAAO|javarodevi  |0055g0000043QwfAAE|null  |F        |viramaram      |2020-05-20 16:01:25.0|1.0               |1936.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
|a045g000001RNLwAAO|arjun ram   |0055g0000043QwfAAE|null  |M        |bagataram      |2020-05-20 16:01:25.0|0.583333          |1981.0          |0055g0000043QwfAAE|2021-03-10 00:00:00|
+------------------+------------+------------------+------+---------+---------------+---------------------+------------------+----------------+------------------+-------------------+
only showing top 10 rows

21/05/29 07:08:41 INFO targetFS.JDBCtoFS: Write data to target path: s3a://testbucketspear/salesforcedata with format: parquet completed with status:success

```

## Target NOSQL
Spear framework supports writing data to NOSQL Databases like mongodb/cassandra etc..

### File source

![image](https://user-images.githubusercontent.com/59328701/120106455-a1397400-c17a-11eb-9ee2-a3b646e76968.png)

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


### JDBC source

![image](https://user-images.githubusercontent.com/59328701/120106053-23289d80-c179-11eb-8a25-8f1691f440de.png)

#### Salesforce to Cassandra Connector
Points to remember:
1. To write data into Cassandra a keyspace and a table must exist as spark will not create them automatically and your connector might fail.
2. The target object must be of the form <keyspace_name>.<table_name>
3. The steps for creating a keyspace and table in Cassandra are also shown in the connector below.

Create keyspace and table in Cassandra:
```commandline

root@2cab4a6ebee2:/# cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.10 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help. 
cqlsh>  CREATE KEYSPACE ingest
  WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

cqlsh:ingest>    CREATE TABLE salesforcedata_new(
          ...                 Id text PRIMARY KEY,
          ...                 name text,
          ...                 OwnerId text,
          ...                 age__c text,
          ...                 gender__c text
          ...                  );
```

Write connector:
```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val cassandraProps = Map(
  "spark.cassandra.connection.host" -> "cassandra")

Logger.getLogger("com.github").setLevel(Level.INFO)
val salseforceToCassandraConnector = SpearConnector
  .createConnector("Salesforce-Cassandra")
  .source(sourceType = "relational", sourceFormat = "soql")
  .target(targetType = "nosql", targetFormat = "cassandra")
  .getConnector

salseforceToCassandraConnector.setVeboseLogging(true)

salseforceToCassandraConnector
  .sourceSql(Map("login" -> "https://login.salesforce.com", "username" -> "salesforce", "password" -> "salesforce"),
    """select
      |    Id,
      |    name,
      |    OwnerId,
      |    age__c,
      |    gender__c,
      |    guardianname__c,
      |    ingest_ts_utc__c,
      |    name_siml_score__c,
      |    year_of_birth__c,
      |    LastModifiedById,
      |    LastModifiedDate
      |    from spark_sale_test__c""".stripMargin)
  .saveAs("__temp__")
  .transformSql(
    """select
      |    cast (Id as STRING) as id,
      |    name,
      |    cast (OwnerId as STRING) as ownerid,
      |    age__c,
      |    gender__c
      |    from __temp__""".stripMargin)
  .targetNoSQL(objectName = "ingest.salesforcedata_new", props = cassandraProps, saveMode = SaveMode.Append)

salseforceToCassandraConnector.stop()   
```



##### Output

```commandline
21/05/30 11:23:54 INFO targetNoSQL.JDBCtoNoSQL: Connector:Salesforce-Cassandra from Source sql: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with Format: soql started running!!
21/05/30 11:24:17 INFO targetNoSQL.JDBCtoNoSQL: Executing source sql query: select
    Id,
    name,
    OwnerId,
    age__c,
    gender__c,
    guardianname__c,
    ingest_ts_utc__c,
    name_siml_score__c,
    year_of_birth__c,
    LastModifiedById,
    LastModifiedDate
    from spark_sale_test__c with format: soql status:success
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|LastModifiedDate            |OwnerId           |age__c|gender__c|name_siml_score__c|guardianname__c|year_of_birth__c|Id                |LastModifiedById  |ingest_ts_utc__c     |Name        |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.583333          |prahlad ram    |1962.0          |a045g000001RNLnAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|magi devi   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |1.0               |kisan lal      |1959.0          |a045g000001RNLoAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|bhanwar lal |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |bhagwan singh  |1967.0          |a045g000001RNLpAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|chain kanvar|
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |null              |pepa ram       |1998.0          |a045g000001RNLqAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|baga ram    |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.333333          |sataram        |1974.0          |a045g000001RNLrAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|prabhu ram  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.357143          |jiyaram        |1981.0          |a045g000001RNLsAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|karna ram   |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |hameera ram    |1992.0          |a045g000001RNLtAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|vimla       |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |0.6               |shyam singh    |1987.0          |a045g000001RNLuAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|brij kanvar |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |F        |1.0               |viramaram      |1936.0          |a045g000001RNLvAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|javarodevi  |
|2021-03-10T10:42:59.000+0000|0055g0000043QwfAAE|null  |M        |0.583333          |bagataram      |1981.0          |a045g000001RNLwAAO|0055g0000043QwfAAE|2020-05-20 16:01:25.0|arjun ram   |
+----------------------------+------------------+------+---------+------------------+---------------+----------------+------------------+------------------+---------------------+------------+
only showing top 10 rows

21/05/30 11:24:17 INFO targetNoSQL.JDBCtoNoSQL: Saving data as temporary table:__temp__ success
21/05/30 11:24:17 INFO targetNoSQL.JDBCtoNoSQL: Executing tranformation sql: select
    cast (Id as STRING) as id,
    name,
    cast (OwnerId as STRING) as ownerid,
    age__c,
    gender__c
    from __temp__ status :success
+------------------+------------+------------------+------+---------+
|id                |name        |ownerid           |age__c|gender__c|
+------------------+------------+------------------+------+---------+
|a045g000001RNLnAAO|magi devi   |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLoAAO|bhanwar lal |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLpAAO|chain kanvar|0055g0000043QwfAAE|null  |F        |
|a045g000001RNLqAAO|baga ram    |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLrAAO|prabhu ram  |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLsAAO|karna ram   |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLtAAO|vimla       |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLuAAO|brij kanvar |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLvAAO|javarodevi  |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLwAAO|arjun ram   |0055g0000043QwfAAE|null  |M        |
+------------------+------------+------------------+------+---------+
only showing top 10 rows

21/05/30 11:24:22 INFO targetNoSQL.JDBCtoNoSQL: Write data to object ingest.salesforcedata_new completed with status:success
+------------------+------------+------------------+------+---------+
|id                |name        |ownerid           |age__c|gender__c|
+------------------+------------+------------------+------+---------+
|a045g000001RNLnAAO|magi devi   |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLoAAO|bhanwar lal |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLpAAO|chain kanvar|0055g0000043QwfAAE|null  |F        |
|a045g000001RNLqAAO|baga ram    |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLrAAO|prabhu ram  |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLsAAO|karna ram   |0055g0000043QwfAAE|null  |M        |
|a045g000001RNLtAAO|vimla       |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLuAAO|brij kanvar |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLvAAO|javarodevi  |0055g0000043QwfAAE|null  |F        |
|a045g000001RNLwAAO|arjun ram   |0055g0000043QwfAAE|null  |M        |
+------------------+------------+------------------+------+---------+
only showing top 10 rows


Cassandra o/p:
==============
cqlsh:ingest> select * from salesforcedata_new;

 id                 | age__c | gender__c | name             | ownerid
--------------------+--------+-----------+------------------+--------------------
 a045g000001ROpXAAW |   null |         M |        sukha ram | 0055g0000043QwfAAE
 a045g000001RPNbAAO |   null |         M |      punam singh | 0055g0000043QwfAAE
 a045g000001RPCkAAO |   null |         M |      radhakishan | 0055g0000043QwfAAE
 a045g000001RNzbAAG |   null |         F |            dhani | 0055g0000043QwfAAE
```

### NoSQL Source

![image](https://user-images.githubusercontent.com/59328701/120106101-5ff49480-c179-11eb-8c99-e3a053c3172d.png)

#### Cassandra to Mongo Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val mongoProps = Map(
  "uri" -> "mongodb://mongo:27017"
)

Logger.getLogger("com.github").setLevel(Level.INFO)  
  
val cassandraTOMongoConnector = SpearConnector
    .createConnector("cassandra-mongo")
    .source(sourceType = "nosql", sourceFormat = "cassandra")
    .target(targetType = "nosql", targetFormat = "mongo")
    .getConnector
cassandraTOMongoConnector.setVeboseLogging(true)

cassandraTOMongoConnector
   .source(sourceObject="ingest.salesforcedata_new",Map("spark.cassandra.connection.host" -> "cassandra"))
   .saveAs("__cassandra_temp__") 
   .transformSql("""select 
      |    id,
      |    cast (name as STRING) as name,
      |    cast (ownerid as STRING) as ownerid,
      |    age__c as age,
      |    cast (gender__c as STRING) as gender 
      |    from __cassandra_temp__""".stripMargin)
   .targetNoSQL(objectName="ingest.cassandra_data_mongo",props=mongoProps,saveMode=SaveMode.Overwrite)
   
cassandraTOMongoConnector.stop()   
```

##### Output

```commandline

21/05/30 12:41:20 INFO targetNoSQL.NoSQLtoNoSQL: Connector:cassandra-mongo to Target:NoSQL with Format:mongo from Source Object:ingest.salesforcedata_new with Format:cassandra started running!!
21/05/30 12:41:20 INFO targetNoSQL.NoSQLtoNoSQL: Reading source object: ingest.salesforcedata_new with format: cassandra status:success
+------------------+------+---------+-------------+------------------+
|id                |age__c|gender__c|name         |ownerid           |
+------------------+------+---------+-------------+------------------+
|a045g000001RPSEAA4|null  |F        |sushil kanvar|0055g0000043QwfAAE|
|a045g000001RNfpAAG|null  |M        |kirat singh  |0055g0000043QwfAAE|
|a045g000001RNfoAAG|null  |M        |gaurakha ram |0055g0000043QwfAAE|
|a045g000001RNtWAAW|null  |M        |dhanna ram   |0055g0000043QwfAAE|
|a045g000001RNiTAAW|null  |F        |udekanvar    |0055g0000043QwfAAE|
|a045g000001RPD2AAO|null  |M        |dinesh kumar |0055g0000043QwfAAE|
|a045g000001ROb3AAG|null  |M        |bhanwar lal  |0055g0000043QwfAAE|
|a045g000001ROi7AAG|null  |F        |santu kanvar |0055g0000043QwfAAE|
|a045g000002GlshAAC|null  |null     |Superman181  |0055g0000043QwfAAE|
|a045g000001RPWXAA4|null  |M        |vishal singh |0055g0000043QwfAAE|
+------------------+------+---------+-------------+------------------+
only showing top 10 rows

21/05/30 12:41:20 INFO targetNoSQL.NoSQLtoNoSQL: Saving data as temporary table:__cassandra_temp__ success
21/05/30 12:41:20 INFO targetNoSQL.NoSQLtoNoSQL: Executing tranformation sql: select
    id,
    cast (name as STRING) as name,
    cast (ownerid as STRING) as ownerid,
    age__c as age,
    cast (gender__c as STRING) as gender
    from __cassandra_temp__ status :success
+------------------+-------------+------------------+----+------+
|id                |name         |ownerid           |age |gender|
+------------------+-------------+------------------+----+------+
|a045g000001RPSEAA4|sushil kanvar|0055g0000043QwfAAE|null|F     |
|a045g000001RNfpAAG|kirat singh  |0055g0000043QwfAAE|null|M     |
|a045g000001RNfoAAG|gaurakha ram |0055g0000043QwfAAE|null|M     |
|a045g000001RNtWAAW|dhanna ram   |0055g0000043QwfAAE|null|M     |
|a045g000001RNiTAAW|udekanvar    |0055g0000043QwfAAE|null|F     |
|a045g000001RPD2AAO|dinesh kumar |0055g0000043QwfAAE|null|M     |
|a045g000001ROb3AAG|bhanwar lal  |0055g0000043QwfAAE|null|M     |
|a045g000001ROi7AAG|santu kanvar |0055g0000043QwfAAE|null|F     |
|a045g000002GlshAAC|Superman181  |0055g0000043QwfAAE|null|null  |
|a045g000001RPWXAA4|vishal singh |0055g0000043QwfAAE|null|M     |
+------------------+-------------+------------------+----+------+
only showing top 10 rows

21/05/30 12:41:21 INFO targetNoSQL.NoSQLtoNoSQL: Write data to object ingest.cassandra_data_mongo completed with status:success
+------------------+-------------+------------------+----+------+
|id                |name         |ownerid           |age |gender|
+------------------+-------------+------------------+----+------+
|a045g000001RPSEAA4|sushil kanvar|0055g0000043QwfAAE|null|F     |
|a045g000001RNfpAAG|kirat singh  |0055g0000043QwfAAE|null|M     |
|a045g000001RNfoAAG|gaurakha ram |0055g0000043QwfAAE|null|M     |
|a045g000001RNtWAAW|dhanna ram   |0055g0000043QwfAAE|null|M     |
|a045g000001RNiTAAW|udekanvar    |0055g0000043QwfAAE|null|F     |
|a045g000001RPD2AAO|dinesh kumar |0055g0000043QwfAAE|null|M     |
|a045g000001ROb3AAG|bhanwar lal  |0055g0000043QwfAAE|null|M     |
|a045g000001ROi7AAG|santu kanvar |0055g0000043QwfAAE|null|F     |
|a045g000002GlshAAC|Superman181  |0055g0000043QwfAAE|null|null  |
|a045g000001RPWXAA4|vishal singh |0055g0000043QwfAAE|null|M     |
+------------------+-------------+------------------+----+------+
only showing top 10 rows
````
# Target GraphDB
Spear framework is also provisioned to write connectors with graph databases as targets from different sources.Spear framework is also provisioned to write connectors with graph databases as targets from different sources.This section has the example connectors form different source to graph databases.The target properties or options for writing to graph databas as target can be refered from [here](https://neo4j.com/developer/spark/writing/).

## File Source
![image](https://user-images.githubusercontent.com/59328701/122415096-3e7f0f80-cfa5-11eb-95ba-9e993b9069e7.png)

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
## JDBC Source
![image](https://user-images.githubusercontent.com/59328701/122404524-ee03b400-cf9c-11eb-83cb-ba7afe81f983.png)

#### Postgres to Neo4j Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val neo4jParams = Map("url" -> "bolt://host:7687",
  "authentication.basic.username" -> "neo4j",
  "authentication.basic.password" -> "****"
)

val postgrestoNeo4j = SpearConnector
  .createConnector("postgres-to-Neo4j")
  .source("relational", "jdbc")
  .target("graph", "neo4j")
  .getConnector
postgrestoNeo4j.setVeboseLogging(true)

postgrestoNeo4j
  .source("employee_details", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("employee")
  .transformSql(
    """
      |select *
      |from employee""".stripMargin)
  .targetGraphDB(objectName = "employee", params = neo4jParams, saveMode = SaveMode.Overwrite)
postgrestoNeo4j.stop()  
```

##### Output

```commandline

21/06/17 13:13:00 INFO targetGraphDB.JDBCtoGraphDB: Connector:postgres-to-Neo4j to Target:GraphB with Format:neo4j from Source table/Object:employee_details with Format:jdbc started running!!
21/06/17 13:13:00 INFO targetGraphDB.JDBCtoGraphDB: Reading source table: employee_details with format: jdbc status:success
+------+--------+-------+---------+
|emp_id|emp_name|emp_add|emp_phone|
+------+--------+-------+---------+
|1     |Ram     |HYD    |1234.0   |
|2     |Ravi    |SEC    |12345.0  |
|3     |Ramesh  |SEC    |345.0    |
|4     |Rao     |BKP    |3445.0   |
|5     |Rahul   |SKP    |344567.0 |
|6     |Raghu   |KP     |231567.0 |
|7     |Ratan   |MGBS   |131567.0 |
+------+--------+-------+---------+

21/06/17 13:13:00 INFO targetGraphDB.JDBCtoGraphDB: Saving data as temporary table:employee success
21/06/17 13:13:00 INFO targetGraphDB.JDBCtoGraphDB: Executing transformation sql:
select *
from employee status :success
+------+--------+-------+---------+
|emp_id|emp_name|emp_add|emp_phone|
+------+--------+-------+---------+
|1     |Ram     |HYD    |1234.0   |
|2     |Ravi    |SEC    |12345.0  |
|3     |Ramesh  |SEC    |345.0    |
|4     |Rao     |BKP    |3445.0   |
|5     |Rahul   |SKP    |344567.0 |
|6     |Raghu   |KP     |231567.0 |
|7     |Ratan   |MGBS   |131567.0 |
+------+--------+-------+---------+

21/06/17 13:13:00 INFO targetGraphDB.JDBCtoGraphDB: Write data to object:employee completed with status:success
+------+--------+-------+---------+
|emp_id|emp_name|emp_add|emp_phone|
+------+--------+-------+---------+
|1     |Ram     |HYD    |1234.0   |
|2     |Ravi    |SEC    |12345.0  |
|3     |Ramesh  |SEC    |345.0    |
|4     |Rao     |BKP    |3445.0   |
|5     |Rahul   |SKP    |344567.0 |
|6     |Raghu   |KP     |231567.0 |
|7     |Ratan   |MGBS   |131567.0 |
+------+--------+-------+---------+
```

## NoSQL Source
![image](https://user-images.githubusercontent.com/59328701/122407124-01178380-cf9f-11eb-8018-0d0134076298.png)

### MongoDB to Neo4j Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val neo4jParams = Map("url" -> "bolt://host:7687",
  "authentication.basic.username" -> "neo4j",
  "authentication.basic.password" -> "speartest",
  "labels" -> "elections_mongo"
)

val mongotoNeo4j = SpearConnector
  .createConnector("Mongodb-to-Neo4j")
  .source("nosql", "mongo")
  .target("graph", "neo4j")
  .getConnector

mongotoNeo4j.setVeboseLogging(true)
mongotoNeo4j
  .source(sourceObject = "ingest.mongo", Map("uri" -> "mongodb://mongodb:27017"))
  .saveAs("__mongo_staging__")
  .transformSql(
    """
      |select country_id,country_name,
      |country_total_votes
      |from __mongo_staging__
      | where state_code='AL'
      |""".stripMargin)
  .targetGraphDB(objectName="elections_mongo",params=neo4jParams,saveMode = SaveMode.Overwrite)
  
mongotoNeo4j.stop()  
```

##### Output
```commandline
21/06/17 13:31:21 INFO targetGraphDB.NOSQLtoGraphDB: Connector:Mongodb-to-Neo4j to Target:GraphDB with Format:neo4j from NoSQL Object:ingest.mongo with Format:mongo started running!!
21/06/17 13:31:21 INFO targetGraphDB.NOSQLtoGraphDB: Reading source object: ingest.mongo with format: mongo status:success
+--------------------------+----------+------------+-------------------+----------+---------+-----+----------+-----+
|_id                       |country_id|country_name|country_total_votes|first_name|last_name|party|state_code|votes|
+--------------------------+----------+------------+-------------------+----------+---------+-----+----------+-----+
|[60cb434ab7d64352ea7932a5]|1         |Alasaba     |220596             |Barack    |Obama    |Dem  |AK        |91696|
|[60cb434ab7d64352ea7932a6]|2         |Akaskak     |220596             |Barack    |Obama    |Dem  |AK        |91696|
|[60cb434ab7d64352ea7932a7]|3         |Autauga     |23909              |Barack    |Obama    |Dem  |AL        |6354 |
|[60cb434ab7d64352ea7932a8]|4         |Akaska      |220596             |Barack    |Obama    |Dem  |AK        |91696|
|[60cb434ab7d64352ea7932a9]|5         |Baldwin     |84988              |Barack    |Obama    |Dem  |AL        |18329|
|[60cb434ab7d64352ea7932aa]|6         |Barbour     |11459              |Barack    |Obama    |Dem  |AL        |5873 |
|[60cb434ab7d64352ea7932ab]|7         |Bibb        |8391               |Barack    |Obama    |Dem  |AL        |2200 |
|[60cb434ab7d64352ea7932ac]|8         |Blount      |23980              |Barack    |Obama    |Dem  |AL        |2961 |
|[60cb434ab7d64352ea7932ad]|9         |Bullock     |5318               |Barack    |Obama    |Dem  |AL        |4058 |
|[60cb434ab7d64352ea7932ae]|10        |Butler      |9483               |Barack    |Obama    |Dem  |AL        |4367 |
+--------------------------+----------+------------+-------------------+----------+---------+-----+----------+-----+
only showing top 10 rows

21/06/17 13:31:21 INFO targetGraphDB.NOSQLtoGraphDB: Saving data as temporary table:__mongo_staging__ success
21/06/17 13:31:21 INFO targetGraphDB.NOSQLtoGraphDB: Executing transformation sql:
select country_id,country_name,
country_total_votes
from __mongo_staging__
where state_code='AL'  status :success
+----------+------------+-------------------+
|country_id|country_name|country_total_votes|
+----------+------------+-------------------+
|3         |Autauga     |23909              |
|5         |Baldwin     |84988              |
|6         |Barbour     |11459              |
|7         |Bibb        |8391               |
|8         |Blount      |23980              |
|9         |Bullock     |5318               |
|10        |Butler      |9483               |
|11        |Calhoun     |46240              |
|12        |Chambers    |14562              |
|13        |Cherokee    |9761               |
+----------+------------+-------------------+
only showing top 10 rows

21/06/17 13:31:22 INFO targetGraphDB.NOSQLtoGraphDB: Write data to object:elections_mongo completed with status:success
+----------+------------+-------------------+
|country_id|country_name|country_total_votes|
+----------+------------+-------------------+
|3         |Autauga     |23909              |
|5         |Baldwin     |84988              |
|6         |Barbour     |11459              |
|7         |Bibb        |8391               |
|8         |Blount      |23980              |
|9         |Bullock     |5318               |
|10        |Butler      |9483               |
|11        |Calhoun     |46240              |
|12        |Chambers    |14562              |
|13        |Cherokee    |9761               |
+----------+------------+-------------------+
only showing top 10 rows
```

# Other Functionalities of Spear

## Merge using executeQuery API 

Using the executeQuery API in spear you can run the join query for merging two sources and then apply required transformations before 
writing it into the target as show in the below diagram.

### Postgres to Hive Connector With executeQuery API

![image](https://user-images.githubusercontent.com/59328701/121480303-8b347a80-c9e8-11eb-8b42-61a9d39b29c1.png)

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

//source-df-1
postgresToHiveConnector
  .source("emp", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("employee")

//source-df-2 with filters
postgresToHiveConnector
  .source("dept", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("department")
  .transformSql(
    """
      |select  deptno,
      | dname
      | from department
      | where deptno in (20,30)""".stripMargin).saveAs("dept_filtered_data")

//executeQuery() api for runing join on the two df's
postgresToHiveConnector
  .executeQuery(
    """
      |select e.empno as id,
      |e.ename as name,
      |e.job as designation,e.sal as salary,
      |d.dname as department
      |from employee e inner join dept_filtered_data d
      |on e.deptno = d.deptno
      |""".stripMargin).saveAs("emp_dept")
  .transformSql(
    """
      |select id,name,
      |department,salary
      |from emp_dept
      |where salary < 3000""".stripMargin)
  .targetFS(destinationFilePath = "/tmp/ingest", saveAsTable = "ingest.emp_sal_report", saveMode = SaveMode.Overwrite)

postgresToHiveConnector.stop()
```
#### Output
```commandline
21/06/09 15:43:59 INFO targetFS.JDBCtoFS: Connector:PostgresToHiveConnector to Target:File System with Format:parquet from Source Object:emp with Format:jdbc started running!!
21/06/09 15:43:59 INFO targetFS.JDBCtoFS: Reading source table: emp with format: jdbc status:success
+-----+------+---------+----+----------+-------+-------+------+
|empno|ename |job      |mgr |hiredate  |sal    |comm   |deptno|
+-----+------+---------+----+----------+-------+-------+------+
|7369 |SMITH |CLERK    |7902|1980-12-17|800.00 |null   |20    |
|7499 |ALLEN |SALESMAN |7698|1981-02-20|1600.00|300.00 |30    |
|7521 |WARD  |SALESMAN |7698|1981-02-22|1250.00|500.00 |30    |
|7566 |JONES |MANAGER  |7839|1981-04-02|2975.00|null   |20    |
|7654 |MARTIN|SALESMAN |7698|1981-09-28|1250.00|1400.00|30    |
|7698 |BLAKE |MANAGER  |7839|1981-05-01|2850.00|null   |30    |
|7782 |CLARK |MANAGER  |7839|1981-06-09|2450.00|null   |10    |
|7788 |SCOTT |ANALYST  |7566|1987-04-19|3000.00|null   |20    |
|7839 |KING  |PRESIDENT|null|1981-11-17|5000.00|null   |10    |
|7844 |TURNER|SALESMAN |7698|1981-09-08|1500.00|0.00   |30    |
+-----+------+---------+----+----------+-------+-------+------+
only showing top 10 rows

21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Saving data as temporary table:employee success
21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Connector to Target: File System with Format: parquet from Source Object: dept with Format: jdbc started running!!
21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Reading source table: dept with format: jdbc status:success
+------+----------+--------+
|deptno|dname     |loc     |
+------+----------+--------+
|10    |ACCOUNTING|NEW YORK|
|20    |RESEARCH  |DALLAS  |
|30    |SALES     |CHICAGO |
|40    |OPERATIONS|BOSTON  |
+------+----------+--------+

21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Saving data as temporary table:department success
21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Executing transformation sql:
select  deptno,
 dname
 from department
 where deptno in (20,30) status :success
+------+--------+
|deptno|dname   |
+------+--------+
|20    |RESEARCH|
|30    |SALES   |
+------+--------+

21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Saving data as temporary table:dept_filtered_data success
21/06/09 15:44:00 INFO targetFS.JDBCtoFS: Executing spark sql:
select e.empno as id,
e.ename as name,
e.job as designation,e.sal as salary,
d.dname as department
from employee e inner join dept_filtered_data d
on e.deptno = d.deptno
 status :success
+----+------+-----------+-------+----------+
|id  |name  |designation|salary |department|
+----+------+-----------+-------+----------+
|7499|ALLEN |SALESMAN   |1600.00|SALES     |
|7521|WARD  |SALESMAN   |1250.00|SALES     |
|7654|MARTIN|SALESMAN   |1250.00|SALES     |
|7698|BLAKE |MANAGER    |2850.00|SALES     |
|7844|TURNER|SALESMAN   |1500.00|SALES     |
|7900|JAMES |CLERK      |950.00 |SALES     |
|7369|SMITH |CLERK      |800.00 |RESEARCH  |
|7566|JONES |MANAGER    |2975.00|RESEARCH  |
|7788|SCOTT |ANALYST    |3000.00|RESEARCH  |
|7876|ADAMS |CLERK      |1100.00|RESEARCH  |
+----+------+-----------+-------+----------+
only showing top 10 rows

21/06/09 15:44:01 INFO targetFS.JDBCtoFS: Saving data as temporary table:emp_dept success
21/06/09 15:44:01 INFO targetFS.JDBCtoFS: Executing transformation sql:
select id,name,
department,salary
from emp_dept
where salary < 3000 status :success
+----+------+----------+-------+
|id  |name  |department|salary |
+----+------+----------+-------+
|7499|ALLEN |SALES     |1600.00|
|7521|WARD  |SALES     |1250.00|
|7654|MARTIN|SALES     |1250.00|
|7698|BLAKE |SALES     |2850.00|
|7844|TURNER|SALES     |1500.00|
|7900|JAMES |SALES     |950.00 |
|7369|SMITH |RESEARCH  |800.00 |
|7566|JONES |RESEARCH  |2975.00|
|7876|ADAMS |RESEARCH  |1100.00|
+----+------+----------+-------+

21/06/09 15:44:05 INFO targetFS.JDBCtoFS: Write data to target path: /tmp/ingest with format: parquet and saved as table ingest.emp_sal_report completed with status:success
+----+------+----------+-------+
|id  |name  |department|salary |
+----+------+----------+-------+
|7499|ALLEN |SALES     |1600.00|
|7521|WARD  |SALES     |1250.00|
|7654|MARTIN|SALES     |1250.00|
|7698|BLAKE |SALES     |2850.00|
|7844|TURNER|SALES     |1500.00|
|7900|JAMES |SALES     |950.00 |
|7369|SMITH |RESEARCH  |800.00 |
|7566|JONES |RESEARCH  |2975.00|
|7876|ADAMS |RESEARCH  |1100.00|
+----+------+----------+-------+
```

## Multi-targets using branch API

Spear also provides you the scope to write to multiple targets using the branch api.Below are the points to remember while writing to multiple targets using spear 
1. While creating connector object use the multitarget option instaed of target as used in case of single target
 ```scala
 val multiTargetConnector = SpearConnector
  .createConnector("<name>")
  .source(sourceType = "<type>", sourceFormat = "<format>")
  .multiTarget //use multitarget instead of .target(<type>,<format>)  
  .getConnector
 ```
2. Give the destination format in the connector logic while writing to the target as shown in the example below
3. Use the branch api for branching the intermediate df to different targets.

Below is the diagramatic representation of the branch api and example connector for the same

### CSV to Multi-Target Connector With branch API

![image](https://user-images.githubusercontent.com/59328701/121474612-ef077500-c9e1-11eb-9e95-d8f82ce310e1.png)

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

val mongoProps = Map(
  "uri" -> "mongodb://mongo:27017"
)

val csvMultiTargetConnector = SpearConnector
  .createConnector("csv-mongo-hive-postgres")
  .source(sourceType = "file", sourceFormat = "csv")
  .multiTarget
  .getConnector

csvMultiTargetConnector.setVeboseLogging(true)

csvMultiTargetConnector
  .source(sourceObject = "file:///opt/spear-framework/data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("_staging_")
  .branch
  .targets(
  //Target-1 mongo target ---give target format while defing the target as shown below
    csvMultiTargetConnector.targetNoSQL(objectName = "ingest.mongo", destFormat = "mongo", props = mongoProps, saveMode=SaveMode.Overwrite),
  //Target-2 transform and write to Hive  
    csvMultiTargetConnector.transformSql(
      """select state_code,party,
        |sum(votes) as total_votes
        |from _staging_
        |group by state_code,party""".stripMargin)
      .targetFS(destinationFilePath = "tmp/ingest/transformed_new", destFormat = "parquet", saveAsTable = "ingest.transformed_new",saveMode=SaveMode.Overwrite),
   //Target-3 postgres target   
    csvMultiTargetConnector.targetJDBC(objectName = "mytable", destFormat = "jdbc", props=targetProps,saveMode=SaveMode.Overwrite)
  )
csvMultiTargetConnector.stop()
```

#### Output

```commandline
21/06/17 12:42:49 INFO targetAny.FiletoAny: MultiTarget connector with name:csv-mongo-hive-postgres from sourceFile:file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with format:csv started running !!
21/06/17 12:42:49 INFO targetAny.FiletoAny: Reading source file:file:///opt/spear-framework/data/us-election-2012-results-by-county.csv with format:csv status:success
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

21/06/17 12:42:49 INFO targetAny.FiletoAny: Saving data as temporary table:_staging_ success
21/06/17 12:42:49 INFO targetAny.FiletoAny: Caching intermediate Dataframe completed with status :success
21/06/17 12:42:50 INFO targetAny.FiletoAny: Write data to object ingest.mongo completed with status:success
21/06/17 12:42:50 INFO targetAny.FiletoAny: Executing transformation sql: 
select state_code,party,
sum(votes) as total_votes
from _staging_
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

21/06/17 12:43:03 INFO targetAny.FiletoAny: Write data to target path:tmp/ingest_test/transformed_new with format:parquet and saved as table:ingest_test.transformed_new completed with status:success
21/06/17 12:43:04 INFO targetAny.FiletoAny: Write data to table/object:csvtable completed with status:success
```

## Merge and batch API combination

#### Multi-source to Multi-Target Connector

This is an example which demonstrates the combination of both the previous features of executeQuery and batch API.

![image](https://user-images.githubusercontent.com/59328701/121520738-e5493600-ca10-11eb-9f72-4a0703360a76.png)


```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val postgresJoinBatchConnector = SpearConnector
  .createConnector("PostgresToHive-MongoConnector")
  .source(sourceType = "relational", sourceFormat = "jdbc")
  .multiTarget
  .getConnector

postgresJoinBatchConnector.setVeboseLogging(true)
//source-df-1
postgresJoinBatchConnector
  .source("emp", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("employee")

//source-df-2 with filters
postgresJoinBatchConnector
  .source("dept", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://postgres:5432/pgdb"))
  .saveAs("department")
  .transformSql(
    """
      |select  deptno,
      | dname
      | from department
      | where deptno in (20,30)""".stripMargin).saveAs("dept_filtered_data")

//executeQuery() api for runing join on the two df's
postgresJoinBatchConnector
  .executeQuery(
    """
      |select e.empno as id,
      |e.ename as name,
      |e.job as designation,e.sal as salary,
      |d.dname as department
      |from employee e inner join dept_filtered_data d
      |on e.deptno = d.deptno
      |""".stripMargin).saveAs("emp_dept")
  .transformSql(
    """
      |select id,name,
      |department,salary
      |from emp_dept
      |where salary < 3000""".stripMargin)
  .branch
  .targets(
    postgresJoinBatchConnector.targetNoSQL(objectName = "ingest.mongo_emp_sal_report", destFormat = "mongo", props = mongoProps, SaveMode.Overwrite),
    postgresJoinBatchConnector.targetFS(destinationFilePath = "/tmp/ingest", destFormat = "parquet", saveAsTable = "ingest.emp_sal_report", saveMode = SaveMode.Overwrite)
  )

postgresJoinBatchConnector.stop()
```

#### Output

```commandline

21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Connector:PostgresToHive-MongoConnector to multiTargets from JDBC source object:emp with Format:jdbc started running!!
21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Reading source table: emp with format: jdbc status:success
+-----+------+---------+----+----------+-------+-------+------+
|empno|ename |job      |mgr |hiredate  |sal    |comm   |deptno|
+-----+------+---------+----+----------+-------+-------+------+
|7369 |SMITH |CLERK    |7902|1980-12-17|800.00 |null   |20    |
|7499 |ALLEN |SALESMAN |7698|1981-02-20|1600.00|300.00 |30    |
|7521 |WARD  |SALESMAN |7698|1981-02-22|1250.00|500.00 |30    |
|7566 |JONES |MANAGER  |7839|1981-04-02|2975.00|null   |20    |
|7654 |MARTIN|SALESMAN |7698|1981-09-28|1250.00|1400.00|30    |
|7698 |BLAKE |MANAGER  |7839|1981-05-01|2850.00|null   |30    |
|7782 |CLARK |MANAGER  |7839|1981-06-09|2450.00|null   |10    |
|7788 |SCOTT |ANALYST  |7566|1987-04-19|3000.00|null   |20    |
|7839 |KING  |PRESIDENT|null|1981-11-17|5000.00|null   |10    |
|7844 |TURNER|SALESMAN |7698|1981-09-08|1500.00|0.00   |30    |
+-----+------+---------+----+----------+-------+-------+------+
only showing top 10 rows

21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Saving data as temporary table:employee success
21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Connector  to multiTargets  from JDBC source object: dept with Format: jdbc started running!!
21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Reading source table: dept with format: jdbc status:success
+------+----------+--------+
|deptno|dname     |loc     |
+------+----------+--------+
|10    |ACCOUNTING|NEW YORK|
|20    |RESEARCH  |DALLAS  |
|30    |SALES     |CHICAGO |
|40    |OPERATIONS|BOSTON  |
+------+----------+--------+

21/06/09 18:40:17 INFO targetAny.JDBCtoAny: Saving data as temporary table:department success
21/06/09 18:40:18 INFO targetAny.JDBCtoAny: Executing transformation sql:
select  deptno,
 dname
 from department
 where deptno in (20,30) status :success
+------+--------+
|deptno|dname   |
+------+--------+
|20    |RESEARCH|
|30    |SALES   |
+------+--------+

21/06/09 18:40:18 INFO targetAny.JDBCtoAny: Saving data as temporary table:dept_filtered_data success
21/06/09 18:40:18 INFO targetAny.JDBCtoAny: Executing spark sql:
select e.empno as id,
e.ename as name,
e.job as designation,e.sal as salary,
d.dname as department
from employee e inner join dept_filtered_data d
on e.deptno = d.deptno
 status :success
+----+------+-----------+-------+----------+
|id  |name  |designation|salary |department|
+----+------+-----------+-------+----------+
|7782|CLARK |MANAGER    |2450.00|ACCOUNTING|
|7839|KING  |PRESIDENT  |5000.00|ACCOUNTING|
|7934|MILLER|CLERK      |1300.00|ACCOUNTING|
|7499|ALLEN |SALESMAN   |1600.00|SALES     |
|7521|WARD  |SALESMAN   |1250.00|SALES     |
|7654|MARTIN|SALESMAN   |1250.00|SALES     |
|7698|BLAKE |MANAGER    |2850.00|SALES     |
|7844|TURNER|SALESMAN   |1500.00|SALES     |
|7900|JAMES |CLERK      |950.00 |SALES     |
|7369|SMITH |CLERK      |800.00 |RESEARCH  |
+----+------+-----------+-------+----------+
only showing top 10 rows

21/06/09 18:40:18 INFO targetAny.JDBCtoAny: Saving data as temporary table:emp_dept success
21/06/09 18:40:19 INFO targetAny.JDBCtoAny: Executing transformation sql:
select id,name,
department,salary
from emp_dept
where salary < 3000 status :success
+----+------+----------+-------+
|id  |name  |department|salary |
+----+------+----------+-------+
|7782|CLARK |ACCOUNTING|2450.00|
|7934|MILLER|ACCOUNTING|1300.00|
|7499|ALLEN |SALES     |1600.00|
|7521|WARD  |SALES     |1250.00|
|7654|MARTIN|SALES     |1250.00|
|7698|BLAKE |SALES     |2850.00|
|7844|TURNER|SALES     |1500.00|
|7900|JAMES |SALES     |950.00 |
|7369|SMITH |RESEARCH  |800.00 |
|7566|JONES |RESEARCH  |2975.00|
+----+------+----------+-------+
only showing top 10 rows

21/06/09 18:40:19 INFO targetAny.JDBCtoAny: caching intermediate Dataframe status :success
21/06/09 18:40:20 INFO targetAny.JDBCtoAny: Write data to object ingest.mongo_emp_sal_report completed with status:success
21/06/09 18:40:23 INFO targetAny.JDBCtoAny: Write data to target path: /tmp/ingest with format: parquet and saved as table ingest.emp_sal_report completed with status:success
```
