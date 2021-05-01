# Spear Framework

A framework which is a built on top of spark and has the ability to extract and load any kind of data with custom
tansformations applied on the raw data,still allowing you to use the features of Apache spark

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
    * [Target FS](#target-fs)
        + [JDBC to Hive Connector](#jdbc-to-hive-connector)
- [Examples](#examples)

## Introduction

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
3.After 2 min you will get a prompt with the scala shell loaded will all the dependencies where you can write your own connector and test it.
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
NOTE: This spark shell is encpsulated with default hadoop/hive environment readily availble to read data from any source and write it to HDFS.

4. You can write your own connector (look at some examples below ) and test it.

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

//target jdbc properties(can be any jdbc target postgres/mysql/sqlserver/oracle etc..)
Logger.getLogger("com.github").setLevel(Level.INFO)
val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")

//connector logic
val csvJdbcConnector =new SpearConnector().source("file", "csv").target("jdbc", "table").getConnector
csvJdbcConnector.init("local[*]", "CSVtoJdbcConnector")
  .source("data/us-election-2012-results-by-county.csv  ", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("__tmp__")
  .transformSql("select state_code,party,first_name,last_name,votes from __tmp__")
  .saveAs("__tmp2__")
  .transformSql("select state_code,party,sum(votes) as total_votes from __tmp2__ group by state_code,party")
  .target("pg_db.destination_us_elections", properties, SaveMode.Overwrite)
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

val jsonJdbcConnector = new SpearConnector().source("file", "json").target("jdbc", "table").getConnector
jsonJdbcConnector.init("local[*]", "JSONtoJDBC")
  .source("data/data.json", Map("multiline" -> "true"))
  .saveAs("__tmptable__")
  .transformSql("select cast(id*10 as integer) as type_id,type from __tmptable__ ")
  .target("pg_db.json_to_jdbc", properties, SaveMode.Overwrite)
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

val xmlJdbcConnector = new SpearConnector().source("file", "xml").target("jdbc", "table").getConnector
xmlJdbcConnector.init("local[*]", "XMLtoJDBC")
  .source("data/data.xml", Map("rootTag" -> "employees", "rowTag" -> "details"))
  .saveAs("tmp")
  .transformSql("select * from tmp ")
  .target("pg_db.xml_to_jdbc", properties, SaveMode.Overwrite)
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

val connector = new SpearConnector().source("file", "tsv").target("jdbc", "table").getConnector
connector.init("local[*]", "TSVtoJDBC")
  .source("data/product_data", Map("sep" -> "\t", "header" -> "true", "inferSchema" -> "true"))
  .saveAs("tmp")
  .transformSql("select * from tmp ")
  .target("pg_db.tsv_to_jdbc", properties, SaveMode.Overwrite)
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

val avroJdbcConnector = new SpearConnector().source("file", "avro").target("jdbc", "table").getConnector

avroJdbcConnector.init("local[*]", "AvrotoJdbcConnector")
  .source("/opt/sample_data.avro")
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
  .target("pgdb.avro_data", properties, SaveMode.Overwrite)

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

val parquetJdbcConnector = new SpearConnector().source("file", "parquet").target("jdbc", "table").getConnector

parquetJdbcConnector.init("local[*]", "CSVtoJdbcConnector")
  .source("data/sample.parquet")
  .saveAs("__tmp__")
  .transformSql("""select flow1,occupancy1,speed1 from __tmp__""")
  .target("pg_db.user_data", properties, SaveMode.Overwrite)
parquetJdbcConnector.stop()
```

### Output

```
21/02/07 09:11:28 INFO FiletoJDBC: Data after reading from parquet file in path : data/sample3.parquet
21/02/07 09:11:31 INFO CodecPool: Got brand-new decompressor [.snappy]
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
### TargetFS

#### JDBC to Hive

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.spark.sql.SaveMode
import java.util.Properties

val targetproperties = new Properties()
targetproperties.put("destination_file_format", "parquet");
targetproperties.put("destination_table_name", "ingest_test.destination_data")

val postgresToHiveConnector = new SpearConnector().source("jdbc", "jdbc").target("FS", "praquet").getConnector

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
  .target("/user/tmp/ingest_test.db", targetproperties, SaveMode.Overwrite)

postgresToHiveConnector.stop()
```

### Output

```commandline
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                 |type_id    |factory_message_process_id   |factory_uuid                        |factory_id    |   engine_id      |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |Modak |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |Modak |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |Modak |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_authtoken           |5             |Modak |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_authtoken           |5             |Modak |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_authtoken           |5             |Modak |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_errorbot            |5             |Modak |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_filter_resolver_jdbc|5             |Modak |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_errorbot            |5             |Modak |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_flowcontroller      |5             |Modak |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows

21/05/01 10:39:31 INFO targetFS.JDBCtoFS: Data is saved as a temporary table by name: __tmp__
21/05/01 10:39:31 INFO targetFS.JDBCtoFS: showing saved data from temporary table with name: __tmp__
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                |type_id    |factory_message_process_id    |factory_uuid                        |factory_id    | engine_id        |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |Modak |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |Modak |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |Modak |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_authtoken           |5             |Modak |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_authtoken           |5             |Modak |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_authtoken           |5             |Modak |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_errorbot            |5             |Modak |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_filter_resolver_jdbc|5             |Modak |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_errorbot            |5             |Modak |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_flowcontroller      |5             |Modak |2021-04-27 10:21:17.780313|
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
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |Modak |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |Modak |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |Modak |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_authtoken           |5             |Modak |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_authtoken           |5             |Modak |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_authtoken           |5             |Modak |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_errorbot            |5             |Modak |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_filter_resolver_jdbc|5             |Modak |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_errorbot            |5             |Modak |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_flowcontroller      |5             |Modak |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows

21/05/01 10:39:35 INFO targetFS.JDBCtoFS: Writing data to target file: /user/tmp/ingest_test.db
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|uuid                                |type_id    |factory_message_process_id    |factory_uuid                        |factory_id    |        engine_id |topic                      |status_code_id|cru_by|cru_ts                    |
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
|null                                |1          |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |factory_2_2                |5             |Modak |2021-04-27 10:17:37.529195|
|null                                |1          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |factory_1_1                |5             |Modak |2021-04-27 10:17:37.533318|
|null                                |1          |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |factory_3_3                |5             |Modak |2021-04-27 10:17:37.535323|
|59d9b23e-ff93-4351-af7e-0a95ec4fde65|10         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_authtoken           |5             |Modak |2021-04-27 10:17:50.441147|
|111eeff6-c61d-402e-9e70-615cf80d3016|10         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_authtoken           |5             |Modak |2021-04-27 10:18:02.439379|
|2870ff43-73c9-424e-9f3c-c89ac4dda278|10         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_authtoken           |5             |Modak |2021-04-27 10:18:14.5242  |
|58fe7575-9c4f-471e-8893-9bc39b4f1be4|18         |1619518658043                 |5ef4bcb3-f064-4532-ad4f-5e8b68c33f70|3             |3                 |nabu_3_errorbot            |5             |Modak |2021-04-27 10:21:17.098984|
|534a2af0-af74-4633-8603-926070afd76f|16         |1619518657679                 |b218b4a2-2723-4a51-a83b-1d9e5e1c79ff|2             |2                 |nabu_2_filter_resolver_jdbc|5             |Modak |2021-04-27 10:21:17.223042|
|9971130b-9ae1-4a53-89ce-aa1932534956|18         |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_errorbot            |5             |Modak |2021-04-27 10:21:17.437489|
|6db9c72f-85b0-4254-bc2f-09dc1e63e6f3|9          |1619518657481                 |ec65395c-fdbc-4697-ac91-bc72447ae7cf|1             |1                 |nabu_1_flowcontroller      |5             |Modak |2021-04-27 10:21:17.780313|
+------------------------------------+-----------+------------------------------+------------------------------------+--------------+------------------+---------------------------+--------------+------+--------------------------+
only showing top 10 rows
```