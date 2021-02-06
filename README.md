# Spear Framework
A framework which is a built on top of spark and has the ability to extract and load any kind of data with custom tansformations applied on the raw data,still allowing you to use the features of Apache spark

## Table of Contents
- [Introduction](#introduction)
- [Requirements](#requirements)
- [Connectors](#connectors)
  * [Target JDBC](#target-jdbc)
    + [CSV to JDBC Connector](#csv-to-jdbc-connector)
    + [JSON to JDBC Connector](#json-to-jdbc-connector)
    + [XML to JDBC Connector](#xml-to-jdbc-connector) 
    + [TSV to JDBC Connector](#tsv-to-jdbc-connector)
    + [Avro to JDBC Connector](#avro-to-jdbc-connector)
    + [Parquet to JDBC Connector](#parquet-to-jdbc-connector)
  * [Target FS](#target-fs)
- [Examples](#examples) 

## Introduction
Spear Framework is basically used to write connectors from source to target,applying business logic/transformations over the soure data and loading it to the corresponding destination

## Connectors
Connector is basically the logic/code with which you can create a pipeline from source to target using the spear framework.Below are the steps to write any connector logic:
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
//target jdbc properties
val properties = new Properties()
properties.put("driver", "org.postgresql.Driver");
properties.put("user", "postgres_user")
properties.put("password", "mysecretpassword")
properties.put("url", "jdbc:postgresql://postgres_host:5433/pg_db")
    
//connector logic 
val csvJdbcConnector = new SpearConnector().source("csv").destination("jdbc").getConnector

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
21/01/26 14:17:12 INFO CSVtoJdbcConnector$: showing data in destination table :
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

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val jsonJdbcConnector = new SpearConnector().sourceType("json").targetType("jdbc").getConnector
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
21/02/06 09:29:33 INFO FiletoJDBC: Showing data for target : pg_db.json_to_jdbc
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

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val xmlJdbcConnector = new SpearConnector().sourceType("xml").targetType("jdbc").getConnector

xmlJdbcConnector.init("local[*]", "XMLtoJDBC")
        .source("data/data.xml", Map("rootTag" -> "employees", "rowTag" -> "details"))
        .saveAs("tmp")
        .transformSql("select * from tmp ")
        .target("pg_db.xml_to_jdbc", properties, SaveMode.Overwrite)
xmlJdbcConnector.stop()
```
##### output

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
21/02/06 12:35:22 INFO FiletoJDBC: Showing data for target : pg_db.xml_to_jdbc
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

```scala
import com.github.edge.roman.spear.SpearConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

val connector = new SpearConnector().sourceType("tsv").targetType("jdbc").getConnector

connector.init("local[*]", "TSVtoJDBC")
      .source("data/product_data", Map("sep" -> "\t", "header" -> "true", "inferSchema" -> "true"))
      .saveAs("tmp")
      .transformSql("select * from tmp ")
      .target("pg_db.tsv_to_jdbc", properties, SaveMode.Overwrite)

connector.stop()
```

##### output
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
21/02/06 12:44:06 INFO FiletoJDBC: Showing data for target : pg_db.tsv_to_jdbc
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