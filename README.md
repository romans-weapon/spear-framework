# Spear Framework

A framework which is a built on top of spark and has the ability to extract and load (ETL or ELT job) any kind of data (structured/unstructured/stream) with custom
tansformations applied on the raw data,still allowing you to use the features of Apache spark.

[![Build Status](https://github.com/AnudeepKonaboina/spear-framework/workflows/spear/badge.svg)](https://github.com/AnudeepKonaboina/spear-framework/actions)

![image](https://user-images.githubusercontent.com/59328701/118262340-64128800-b4d2-11eb-879f-64771d00274c.png)


## Table of Contents

- [Introduction](#introduction)
- [How to Run](#how-to-run)
- [Connectors](#connectors)
    * [Target JDBC](#target-jdbc)
        + [CSV to JDBC Connector](#csv-to-jdbc-connector)
        + [JSON to JDBC Connector](#json-to-jdbc-connector)
        + [XML to JDBC Connector](#xml-to-jdbc-connector)
        + [TSV to JDBC Connector](#tsv-to-jdbc-connector)
        + [Avro to JDBC Connector](#avro-to-jdbc-connector)
        + [Parquet to JDBC Connector](#parquet-to-jdbc-connector)
        + [Oracle to JDBC Connector](#oracle-to-jdbc-connector)
    * [Target FS](#target-fs)
        + [JDBC to Hive Connector](#jdbc-to-hive-connector)
        + [Oracle to Hive Connector](#oracle-to-hive-connector)
- [Examples](#examples)

## Introductionl

Spear Framework is basically used to write connectors from source to target,applying business logic/transformations over
the soure data and loading it to the corresponding destination

## How to Run

Below are the steps to write and run your own connector:

1. Clone the repository from git

```commandline
git clone https://github.com/AnudeepKonaboina/spear-framework.git
```

2. Run setup.sh script using the command

```commandline
sh setup.sh
```

3.After 2 min you will get a prompt with the scala shell loaded will all the dependencies where you can write your own
connector and test it.

```commandline
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Scala version 2.11.12 (OpenJDK 64-Bit Server VM, Java 1.8.0_292)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

4. To run manually,follow the below steps:

For docker conatiners run the below commands one after the other:

```
docker exec -it spark bash -to enter into the container

cd /opt/spear-framework/target/scala-2.12 -path where the binary exists

spark-shell --jars spear-framework_2.12-0.1.jar --packages "org.postgresql:postgresql:9.4.1211,org.apache.spark:spark-hive_2.11:2.4.0" -opens a spark shell on top of spear
```

NOTE: This spark shell is encpsulated with default hadoop/hive environment readily availble to read data from any source
and write it to HDFS.

To run on any on any terminal or linux machine

```
clone the project using git clone https://github.com/AnudeepKonaboina/spear-framework.git

Run cd spear-framework/ and then run sbt pcakage 

Once the jar is created in the target dir ,navigate to the target dir and run the following command:
spark-shell --jars spear-framework_2.12-0.1.jar --packages "org.postgresql:postgresql:9.4.1211,org.apache.spark:spark-hive_2.11:2.4.0"

```

5. You can write your own connector (look at some examples below ) and test it.

## Connectors

Connector is basically the logic/code with which you can create a pipeline from source to target using the spear
framework.Below are the steps to write any connector logic:

1. Get the suitable connector object for the source and destination provided
2. Write the connector logic.
3. On completion stop the connector.

### Target JDBC

#### CSV to JDBC Connector

Connector for reading csv file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/us-election-2012-results-by-county.csv

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)
val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")

//connector logic
val csvJdbcConnector = new SpearConnector().source(sourceType = "file", sourceFormat = "csv").target(targetType = "relational", targetFormat = "jdbc").getConnector
csvJdbcConnector.init(master = "local[*]", appName = "CSVtoJdbcConnector")
  .source(sourceObject = "/user/hive/warehouse/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("__tmp__")
  .transformSql("select state_code,party,first_name,last_name,votes from __tmp__")
  .saveAs("__tmp2__")
  .transformSql(
    """select state_code,party,
      |sum(votes) as total_votes
      |from __tmp2__
      |group by state_code,party""".stripMargin)
  .targetJDBC(tableName = "pg_db.destination_us_elections", properties, SaveMode.Overwrite)
csvJdbcConnector.stop()
```

##### Output:

```
21/01/26 14:16:57 INFO FiletoJDBC: Data after reading from csv in path : data/us-election-2012-results-by-county.csv  
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

21/01/26 14:16:58 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp__
21/01/26 14:16:58 INFO FiletoJDBC: select * from __tmp__
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

21/01/26 14:16:59 INFO FiletoJDBC: Data after transformation using the SQL : select state_code,party,first_name,last_name,votes from __tmp__
+----------+-----+----------+---------+-----+
|state_code|party|first_name|last_name|votes|
+----------+-----+----------+---------+-----+
|AK        |Dem  |Barack    |Obama    |91696|
|AK        |Dem  |Barack    |Obama    |91696|
|AL        |Dem  |Barack    |Obama    |6354 |
|AK        |Dem  |Barack    |Obama    |91696|
|AL        |Dem  |Barack    |Obama    |18329|
|AL        |Dem  |Barack    |Obama    |5873 |
|AL        |Dem  |Barack    |Obama    |2200 |
|AL        |Dem  |Barack    |Obama    |2961 |
|AL        |Dem  |Barack    |Obama    |4058 |
|AL        |Dem  |Barack    |Obama    |4367 |
+----------+-----+----------+---------+-----+
only showing top 10 rows

21/01/26 14:16:59 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp2__
21/01/26 14:16:59 INFO FiletoJDBC: select * from __tmp2__
+----------+-----+----------+---------+-----+
|state_code|party|first_name|last_name|votes|
+----------+-----+----------+---------+-----+
|AK        |Dem  |Barack    |Obama    |91696|
|AK        |Dem  |Barack    |Obama    |91696|
|AL        |Dem  |Barack    |Obama    |6354 |
|AK        |Dem  |Barack    |Obama    |91696|
|AL        |Dem  |Barack    |Obama    |18329|
|AL        |Dem  |Barack    |Obama    |5873 |
|AL        |Dem  |Barack    |Obama    |2200 |
|AL        |Dem  |Barack    |Obama    |2961 |
|AL        |Dem  |Barack    |Obama    |4058 |
|AL        |Dem  |Barack    |Obama    |4367 |
+----------+-----+----------+---------+-----+
only showing top 10 rows

21/01/26 14:16:59 INFO FiletoJDBC: Data after transformation using the SQL : select state_code,party,sum(votes) as total_votes from __tmp2__ group by state_code,party
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

21/01/26 14:17:02 INFO FiletoJDBC: Writing data to target table: pg_db.destination_us_elections
21/01/26 14:17:12 INFO FiletoJDBC: Showing data in target table  : pg_db.destination_us_elections
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|AL        |Dem  |793620     |
|MN        |Grn  |13045      |
|NY        |GOP  |2226637    |
|MI        |CST  |16792      |
|ID        |GOP  |420750     |
|ID        |Ind  |2495       |
|WA        |CST  |7772       |
|HI        |Grn  |3121       |
|MS        |RP   |969        |
|ID        |Dem  |212560     |
+----------+-----+-----------+
only showing top 10 rows
```

#### JSON to JDBC Connector

Connector for reading json file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/data.json

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

//target props
val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")

val jsonJdbcConnector = new SpearConnector().source(sourceType = "file", sourceFormat = "json").target(targetType = "relational", targetFormat = "jdbc").getConnector
jsonJdbcConnector.init(master = "local[*]", appName = "JSONtoJDBC")
  .source("data/data.json", Map("multiline" -> "true"))
  .saveAs("__tmptable__")
  .transformSql("select cast(id*10 as integer) as type_id,type from __tmptable__ ")
  .targetJDBC(tableName = "pg_db.json_to_jdbc", properties, SaveMode.Overwrite)

jsonJdbcConnector.stop()
```

##### Output

```
21/02/06 09:29:29 INFO FiletoJDBC: Data after reading from json file in path : data/data.json
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

21/02/06 09:29:31 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmptable__
21/02/06 09:29:31 INFO FiletoJDBC: select * from __tmptable__
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

21/02/06 09:29:32 INFO FiletoJDBC: Data after transformation using the SQL : select cast(id*10 as integer) as type_id,type from __tmptable__ 
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

21/02/06 09:29:32 INFO FiletoJDBC: Writing data to target table: pg_db.json_to_jdbc
21/02/06 09:29:33 INFO FiletoJDBC: Showing data in target table  : pg_db.json_to_jdbc
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

#### XML to JDBC Connector

Connector for reading xml file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/data.xml

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")

val xmlJdbcConnector = new SpearConnector().source(sourceType = "file", sourceFormat = "xml").target(targetType = "relational", targetFormat = "jdbc").getConnector
xmlJdbcConnector.init(master = "local[*]", appName = "XMLtoJDBC")
  .source("data/data.xml", Map("rootTag" -> "employees", "rowTag" -> "details"))
  .saveAs("tmp")
  .transformSql("select * from tmp ")
  .targetJDBC(tableName="pg_db.xml_to_jdbc", properties, SaveMode.Overwrite)
xmlJdbcConnector.stop()
```

##### Output

```
21/02/06 12:35:17 INFO FiletoJDBC: Data after reading from xml file in path : data/data.xml
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+

21/02/06 12:35:19 INFO FiletoJDBC: Data is saved as a temporary table by name: tmp
21/02/06 12:35:19 INFO FiletoJDBC: select * from tmp
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+

21/02/06 12:35:20 INFO FiletoJDBC: Data after transformation using the SQL : select * from tmp 
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+

21/02/06 12:35:21 INFO FiletoJDBC: Writing data to target table: pg_db.xml_to_jdbc
21/02/06 12:35:22 INFO FiletoJDBC: Showing data in target table  : pg_db.xml_to_jdbc
+--------+--------+---------+--------+----+---------+
|building|division|firstname|lastname|room|title    |
+--------+--------+---------+--------+----+---------+
|301     |Computer|Shiv     |Mishra  |11  |Engineer |
|303     |Computer|Yuh      |Datta   |2   |developer|
|304     |Computer|Rahil    |Khan    |10  |Tester   |
|305     |Computer|Deep     |Parekh  |14  |Designer |
+--------+--------+---------+--------+----+---------+
```

#### TSV to JDBC Connector

Connector for reading csv file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/product_data

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")

val connector = new SpearConnector().source(sourceType = "file", sourceFormat = "tsv").target(targetType = "relational", targetFormat = "jdbc").getConnector
connector.init(master = "local[*]", appName = "TSVtoJDBC")
  .source("data/product_data", Map("sep" -> "\t", "header" -> "true", "inferSchema" -> "true"))
  .saveAs("tmp")
  .transformSql("select * from tmp ")
  .targetJDBC(tableName = "pg_db.tsv_to_jdbc", properties, SaveMode.Overwrite)
connector.stop()
```

##### Output

```
21/02/06 12:43:54 INFO FiletoJDBC: Data after reading from tsv file in path : data/product_data
+---+---+------+---------+
|id |num|rating|reviews  |
+---+---+------+---------+
|196|242|3     |881250949|
|186|302|3     |891717742|
|22 |377|1     |878887116|
|244|51 |2     |880606923|
|166|346|1     |886397596|
|298|474|4     |884182806|
|115|265|2     |881171488|
|253|465|5     |891628467|
|305|451|3     |886324817|
|6  |86 |3     |883603013|
+---+---+------+---------+
only showing top 10 rows

21/02/06 12:43:55 INFO FiletoJDBC: Data is saved as a temporary table by name: tmp
21/02/06 12:43:55 INFO FiletoJDBC: select * from tmp
+---+---+------+---------+
|id |num|rating|reviews  |
+---+---+------+---------+
|196|242|3     |881250949|
|186|302|3     |891717742|
|22 |377|1     |878887116|
|244|51 |2     |880606923|
|166|346|1     |886397596|
|298|474|4     |884182806|
|115|265|2     |881171488|
|253|465|5     |891628467|
|305|451|3     |886324817|
|6  |86 |3     |883603013|
+---+---+------+---------+
only showing top 10 rows

21/02/06 12:43:55 INFO FiletoJDBC: Data after transformation using the SQL : select * from tmp 
+---+---+------+---------+
|id |num|rating|reviews  |
+---+---+------+---------+
|196|242|3     |881250949|
|186|302|3     |891717742|
|22 |377|1     |878887116|
|244|51 |2     |880606923|
|166|346|1     |886397596|
|298|474|4     |884182806|
|115|265|2     |881171488|
|253|465|5     |891628467|
|305|451|3     |886324817|
|6  |86 |3     |883603013|
+---+---+------+---------+
only showing top 10 rows

21/02/06 12:43:56 INFO FiletoJDBC: Writing data to target table: pg_db.tsv_to_jdbc
21/02/06 12:44:06 INFO FiletoJDBC: Showing data in target table  : pg_db.tsv_to_jdbc
+---+---+------+---------+
|id |num|rating|reviews  |
+---+---+------+---------+
|196|242|3     |881250949|
|186|302|3     |891717742|
|22 |377|1     |878887116|
|244|51 |2     |880606923|
|166|346|1     |886397596|
|298|474|4     |884182806|
|115|265|2     |881171488|
|253|465|5     |891628467|
|305|451|3     |886324817|
|6  |86 |3     |883603013|
+---+---+------+---------+
only showing top 10 rows
```

#### Avro to JDBC Connector

Connector for reading avro file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/sample_data.avro

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.SaveMode
import scala.collection.JavaConverters._
import java.util.Properties

val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres")
properties.put("password", "pass")
properties.put("url", "jdbc:postgresql://localhost:5432/pgdb")

val avroJdbcConnector = new SpearConnector().source(sourceType="file", sourceFormat="avro").target(targetType="relational", targetFormat="jdbc").getConnector
avroJdbcConnector.init(master = "local[*]", appName = "AvrotoJdbcConnector")
  .source(sourceObject = "/user/hive/warehouse/sample_data.avro")
  .saveAs("__tmp__")
  .transformSql(
    """select id,
      |cast(concat(first_name ,' ', last_name) as VARCHAR(255)) as name,
      |coalesce(gender,'NA') as gender,
      |cast(country as VARCHAR(20)) as country,
      |cast(salary as DOUBLE) as salary,email
      |from __tmp__""".stripMargin)
  .saveAs("__transformed_table__")
  .transformSql("select id,name,country,email,salary from __transformed_table__ ")
  .targetJDBC(tableName = "nabu.avro_data", properties, SaveMode.Overwrite)

avroJdbcConnector.stop()
```

##### Output

```
21/02/07 08:35:25 INFO FiletoJDBC: Data after reading from avro file in path : data/sample_data.avro
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

21/02/07 08:35:29 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp__
21/02/07 08:35:29 INFO FiletoJDBC: showing saved data from temporary table with name: __tmp__
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

21/02/07 08:35:29 INFO FiletoJDBC: Data after transformation using the SQL : select id,
cast(concat(first_name ,' ', last_name) as VARCHAR(255)) as name,
coalesce(gender,'NA') as gender,
cast(country as VARCHAR(20)) as country,
cast(salary as DOUBLE) as salary,email
from __tmp__
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

21/02/07 08:35:30 INFO FiletoJDBC: Data is saved as a temporary table by name: __transformed_table__
21/02/07 08:35:30 INFO FiletoJDBC: showing saved data from temporary table with name: __transformed_table__
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

21/02/07 08:35:30 INFO FiletoJDBC: Data after transformation using the SQL : select id,name,country,email,salary from __transformed_table__ 
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

21/02/07 08:35:31 INFO FiletoJDBC: Writing data to target table: pg_db.company_data
21/02/07 08:35:32 INFO FiletoJDBC: Showing data in target table  : pg_db.company_data
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

#### Parquet to JDBC Connector

Connector for reading parquet file applying transformations and storing it into postgres table using spear:\
The input data is available in the data/sample.parquet

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres")
properties.put("password", "pass")
properties.put("url", "jdbc:postgresql://localhost:5432/pgdb")

val parquetJdbcConnector = new SpearConnector().source(sourceType="file", sourceFormat="parquet").target(targetType="relational", targetFormat="jdbc").getConnector
parquetJdbcConnector.init(master="local[*]",appName= "CSVtoJdbcConnector")
  .source("data/sample.parquet")
  .saveAs("__tmp__")
  .transformSql("""select flow1,occupancy1,speed1 from __tmp__""")
  .targetJDBC(tableName="pg_db.user_data", properties, SaveMode.Overwrite)
parquetJdbcConnector.stop()
```

### Output

```
21/02/07 09:11:28 INFO FiletoJDBC: Data after reading from parquet file in path : data/sample3.parquet
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
|timeperiod         |flow1|occupancy1|speed1|flow2|occupancy2|speed2|flow3|occupancy3|speed3|flow4|occupancy4|speed4|flow5|occupancy5|speed5|flow6|occupancy6|speed6|flow7|occupancy7|speed7|flow8|occupancy8|speed8|
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
|09/24/2016 00:00:11|2    |0.01      |78.0  |1    |0.01      |71.0  |3    |0.02      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:00:41|2    |0.01      |71.0  |6    |0.04      |71.0  |2    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:01:11|1    |0.01      |71.0  |0    |0.0       |0.0   |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:01:41|5    |0.03      |78.0  |4    |0.03      |65.0  |4    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:02:11|4    |0.02      |78.0  |1    |0.01      |65.0  |1    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:02:41|2    |0.01      |78.0  |5    |0.03      |71.0  |1    |0.01      |78.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:03:11|1    |0.01      |71.0  |2    |0.01      |71.0  |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:03:41|3    |0.01      |78.0  |2    |0.01      |71.0  |2    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:04:11|0    |0.0       |0.0   |5    |0.03      |71.0  |0    |0.0       |0.0   |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:04:41|1    |0.0       |86.0  |3    |0.02      |78.0  |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
only showing top 10 rows

21/02/07 09:11:32 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp__
21/02/07 09:11:32 INFO FiletoJDBC: showing saved data from temporary table with name: __tmp__
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
|timeperiod         |flow1|occupancy1|speed1|flow2|occupancy2|speed2|flow3|occupancy3|speed3|flow4|occupancy4|speed4|flow5|occupancy5|speed5|flow6|occupancy6|speed6|flow7|occupancy7|speed7|flow8|occupancy8|speed8|
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
|09/24/2016 00:00:11|2    |0.01      |78.0  |1    |0.01      |71.0  |3    |0.02      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:00:41|2    |0.01      |71.0  |6    |0.04      |71.0  |2    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:01:11|1    |0.01      |71.0  |0    |0.0       |0.0   |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:01:41|5    |0.03      |78.0  |4    |0.03      |65.0  |4    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:02:11|4    |0.02      |78.0  |1    |0.01      |65.0  |1    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:02:41|2    |0.01      |78.0  |5    |0.03      |71.0  |1    |0.01      |78.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:03:11|1    |0.01      |71.0  |2    |0.01      |71.0  |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:03:41|3    |0.01      |78.0  |2    |0.01      |71.0  |2    |0.04      |65.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:04:11|0    |0.0       |0.0   |5    |0.03      |71.0  |0    |0.0       |0.0   |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
|09/24/2016 00:04:41|1    |0.0       |86.0  |3    |0.02      |78.0  |2    |0.01      |71.0  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |null |null      |null  |
+-------------------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+-----+----------+------+
only showing top 10 rows

21/02/07 09:11:32 INFO FiletoJDBC: Data after transformation using the SQL : select flow1,occupancy1,speed1 from __tmp__
+-----+----------+------+
|flow1|occupancy1|speed1|
+-----+----------+------+
|2    |0.01      |78.0  |
|2    |0.01      |71.0  |
|1    |0.01      |71.0  |
|5    |0.03      |78.0  |
|4    |0.02      |78.0  |
|2    |0.01      |78.0  |
|1    |0.01      |71.0  |
|3    |0.01      |78.0  |
|0    |0.0       |0.0   |
|1    |0.0       |86.0  |
+-----+----------+------+
only showing top 10 rows

21/02/07 09:11:33 INFO FiletoJDBC: Writing data to target table: pg_db.user_data
21/02/07 09:11:34 INFO FiletoJDBC: Showing data in target table  : pg_db.user_data
+-----+----------+------+
|flow1|occupancy1|speed1|
+-----+----------+------+
|2    |0.01      |78.0  |
|2    |0.01      |71.0  |
|1    |0.01      |71.0  |
|5    |0.03      |78.0  |
|4    |0.02      |78.0  |
|2    |0.01      |78.0  |
|1    |0.01      |71.0  |
|3    |0.01      |78.0  |
|0    |0.0       |0.0   |
|1    |0.0       |86.0  |
+-----+----------+------+
only showing top 10 rows
```

#### Oracle to JDBC Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import java.util.Properties

val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres")
properties.put("password", "pass")
properties.put("url", "jdbc:postgresql://localhost:5432/pgdb")

Logger.getLogger("com.github").setLevel(Level.INFO)

val oracleTOPostgresConnector = new SpearConnector().source(sourceType = "relational", sourceFormat = "jdbc").target(targetType = "relational", targetFormat = "jdbc").getConnector
oracleTOPostgresConnector.init(master = "local[*]", appName = "OracleTOPostgresConnector")
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
  .targetJDBC(tableName = "pgdb.ora_to_postgres", properties, SaveMode.Overwrite)

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
        from NABU_SWAT_DBSRV.ORACLE_TIMESTAMPS

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

21/05/04 17:35:50 INFO targetjdbc.JDBCtoJDBC: Writing data to target table: nabu.ora_to_postgres
21/05/04 17:35:56 INFO targetjdbc.JDBCtoJDBC: Showing data in target table  : nabu.ora_to_postgres
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|ingest_ts_utc             |timestamp_0          |timestamp_5              |timestamp_7                |timestamp_9                  |timestamp0_with_tz                 |timestamp0_with_tz_utc|timestamp5_with_tz                       |timestamp5_with_tz_utc   |timestamp8_with_tz                          |timestamp8_with_tz_utc      |
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
|2021-05-04 17:35:52.709503|2021-04-07 15:15:16.0|2021-04-07 15:15:16.03356|2021-04-07 15:15:16.0335610|2021-04-07 15:15:16.033561000|07-APR-21 03.15.16 PM ASIA/CALCUTTA|2021-04-07 09:45:16   |07-APR-21 03.15.16.03356 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356|07-APR-21 03.15.16.03356100 PM ASIA/CALCUTTA|2021-04-07 09:45:16.03356100|
|2021-05-04 17:35:52.709503|2021-04-07 15:16:51.6|2021-04-07 15:16:51.60911|2021-04-07 15:16:51.6091090|2021-04-07 15:16:51.609109000|07-APR-21 03.16.52 PM ASIA/CALCUTTA|2021-04-07 09:46:52   |07-APR-21 03.16.51.60911 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60911|07-APR-21 03.16.51.60910900 PM ASIA/CALCUTTA|2021-04-07 09:46:51.60910900|
+--------------------------+---------------------+-------------------------+---------------------------+-----------------------------+-----------------------------------+----------------------+-----------------------------------------+-------------------------+--------------------------------------------+----------------------------+
```


### Target FS

#### JDBC to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.SaveMode
import org.apache.log4j.{Level, Logger}
import java.util.Properties

Logger.getLogger("com.github").setLevel(Level.INFO)

val postgresToHiveConnector = new SpearConnector().source(sourceType = "relational", sourceFormat = "jdbc").target(targetType = "FS", targetFormat = "parquet").getConnector
postgresToHiveConnector.init("local[*]", "JdbctoHiveConnector")
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
  .targetFS(destinationFilePath = "/tmp/ingest_test.db", saveAsTable = "ingest_test.postgres_data", SaveMode.Overwrite)

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
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_errorbot            |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_errorbot            |5             |ABCDE |2021-04-27 10:21:17.437489|
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
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_errorbot            |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_errorbot            |5             |ABCDE |2021-04-27 10:21:17.437489|
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
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_errorbot            |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_errorbot            |5             |ABCDE |2021-04-27 10:21:17.437489|
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
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |bale_3_errorbot            |5             |ABCDE |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |bale_2_filter_resolver_jdbc|5             |ABCDE |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_errorbot            |5             |ABCDE |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |bale_1_flowcontroller      |5             |ABCDE |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows
```

#### Oracle to Hive Connector

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

Logger.getLogger("com.github").setLevel(Level.INFO)

val oraToHiveConnector = new SpearConnector().source(sourceType = "relational", sourceFormat = "jdbc").target(targetType = "FS", targetFormat = "parquet").getConnector
oraToHiveConnector.init(master = "local[*]", appName = "OracleToHiveConnector")
oraToHiveConnector.sourceSql(Map("driver" -> "oracle.jdbc.driver.OracleDriver", "user" -> "user", "password" -> "pass", "url" -> "jdbc:oracle:thin:@ora-host:1521:orcl"),
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
  .targetFS(destinationFilePath = "/tmp/ingest_test.db", saveAsTable = "ingest_test.ora_data", SaveMode.Overwrite)
```

### Output

```commandline
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
        from NABU_SWAT_DBSRV.ORACLE_NUMBER

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
