# Spear Framework

Code for reading csv file applying transformations and storing it into postgres table using spear:
```
 //get the suitable connector opject for the source and destination provided
    val connector = new SpearConnector().source("csv").destination("jdbc").getConnector

    //connector logic
    connector.init("local[*]", "CSVtoJdbcConnector")
      .source("data/us-election-2012-results-by-county.csv  ", Map("header" -> "true", "inferSchema" -> "true"))
      .saveAs("__tmp__")
      .transformSql("select state_code,party,first_name,last_name,votes from __tmp__")
      .saveAs("__tmp2__")
      .transformSql("select state_code,party,sum(votes) as total_votes from __tmp2__ group by state_code,party")
      .target("postgres_db.destination_us_elections", properties, SaveMode.Overwrite)

    //stop connector after the data is moved
    connector.stop()
```


Output:
```
21/01/26 14:16:57 INFO FiletoJDBC: Data after reading from csv in path : data/us-election-2012-results-by-county.csv  
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|country_id|state_code|country_name|country_total_votes|party|first_name|last_name|votes|
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|2         |AK        |Alaska      |220596             |Dem  |Barack    |Obama    |91696|
|4         |AL        |Autauga     |23909              |Dem  |Barack    |Obama    |6354 |
|5         |AL        |Baldwin     |84988              |Dem  |Barack    |Obama    |18329|
|6         |AL        |Barbour     |11459              |Dem  |Barack    |Obama    |5873 |
|7         |AL        |Bibb        |8391               |Dem  |Barack    |Obama    |2200 |
|8         |AL        |Blount      |23980              |Dem  |Barack    |Obama    |2961 |
|9         |AL        |Bullock     |5318               |Dem  |Barack    |Obama    |4058 |
|10        |AL        |Butler      |9483               |Dem  |Barack    |Obama    |4367 |
|11        |AL        |Calhoun     |46240              |Dem  |Barack    |Obama    |15500|
|12        |AL        |Chambers    |14562              |Dem  |Barack    |Obama    |6853 |
+----------+----------+------------+-------------------+-----+----------+---------+-----+
only showing top 10 rows



21/01/26 14:16:58 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp__
21/01/26 14:16:58 INFO FiletoJDBC: select * from __tmp__
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|country_id|state_code|country_name|country_total_votes|party|first_name|last_name|votes|
+----------+----------+------------+-------------------+-----+----------+---------+-----+
|         2|        AK|      Alaska|             220596|  Dem|    Barack|    Obama|91696|
|         4|        AL|     Autauga|              23909|  Dem|    Barack|    Obama| 6354|
|         5|        AL|     Baldwin|              84988|  Dem|    Barack|    Obama|18329|
|         6|        AL|     Barbour|              11459|  Dem|    Barack|    Obama| 5873|
|         7|        AL|        Bibb|               8391|  Dem|    Barack|    Obama| 2200|
|         8|        AL|      Blount|              23980|  Dem|    Barack|    Obama| 2961|
|         9|        AL|     Bullock|               5318|  Dem|    Barack|    Obama| 4058|
|        10|        AL|      Butler|               9483|  Dem|    Barack|    Obama| 4367|
|        11|        AL|     Calhoun|              46240|  Dem|    Barack|    Obama|15500|
|        12|        AL|    Chambers|              14562|  Dem|    Barack|    Obama| 6853|
|        13|        AL|    Cherokee|               9761|  Dem|    Barack|    Obama| 2126|
|        14|        AL|     Chilton|              17434|  Dem|    Barack|    Obama| 3391|
|        15|        AL|     Choctaw|               7965|  Dem|    Barack|    Obama| 3785|
|        16|        AL|      Clarke|              13827|  Dem|    Barack|    Obama| 6317|
|        17|        AL|        Clay|               6640|  Dem|    Barack|    Obama| 1770|
|        18|        AL|    Cleburne|               6302|  Dem|    Barack|    Obama|  971|
|        19|        AL|      Coffee|              19715|  Dem|    Barack|    Obama| 4899|
|        20|        AL|     Colbert|              23374|  Dem|    Barack|    Obama| 9160|
|        21|        AL|     Conecuh|               7013|  Dem|    Barack|    Obama| 3551|
|        22|        AL|       Coosa|               5243|  Dem|    Barack|    Obama| 2188|
+----------+----------+------------+-------------------+-----+----------+---------+-----+
only showing top 20 rows

21/01/26 14:16:59 INFO FiletoJDBC: Data after transformation using the SQL : select state_code,party,first_name,last_name,votes from __tmp__
+----------+-----+----------+---------+-----+
|state_code|party|first_name|last_name|votes|
+----------+-----+----------+---------+-----+
|AK        |Dem  |Barack    |Obama    |91696|
|AL        |Dem  |Barack    |Obama    |6354 |
|AL        |Dem  |Barack    |Obama    |18329|
|AL        |Dem  |Barack    |Obama    |5873 |
|AL        |Dem  |Barack    |Obama    |2200 |
|AL        |Dem  |Barack    |Obama    |2961 |
|AL        |Dem  |Barack    |Obama    |4058 |
|AL        |Dem  |Barack    |Obama    |4367 |
|AL        |Dem  |Barack    |Obama    |15500|
|AL        |Dem  |Barack    |Obama    |6853 |
+----------+-----+----------+---------+-----+
only showing top 10 rows

21/01/26 14:16:59 INFO FiletoJDBC: Data is saved as a temporary table by name: __tmp2__
21/01/26 14:16:59 INFO FiletoJDBC: select * from __tmp2__
+----------+-----+----------+---------+-----+
|state_code|party|first_name|last_name|votes|
+----------+-----+----------+---------+-----+
|        AK|  Dem|    Barack|    Obama|91696|
|        AL|  Dem|    Barack|    Obama| 6354|
|        AL|  Dem|    Barack|    Obama|18329|
|        AL|  Dem|    Barack|    Obama| 5873|
|        AL|  Dem|    Barack|    Obama| 2200|
|        AL|  Dem|    Barack|    Obama| 2961|
|        AL|  Dem|    Barack|    Obama| 4058|
|        AL|  Dem|    Barack|    Obama| 4367|
|        AL|  Dem|    Barack|    Obama|15500|
|        AL|  Dem|    Barack|    Obama| 6853|
|        AL|  Dem|    Barack|    Obama| 2126|
|        AL|  Dem|    Barack|    Obama| 3391|
|        AL|  Dem|    Barack|    Obama| 3785|
|        AL|  Dem|    Barack|    Obama| 6317|
|        AL|  Dem|    Barack|    Obama| 1770|
|        AL|  Dem|    Barack|    Obama|  971|
|        AL|  Dem|    Barack|    Obama| 4899|
|        AL|  Dem|    Barack|    Obama| 9160|
|        AL|  Dem|    Barack|    Obama| 3551|
|        AL|  Dem|    Barack|    Obama| 2188|
+----------+-----+----------+---------+-----+
only showing top 20 rows

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

21/01/26 14:17:02 INFO FiletoJDBC: Writing data to target table: postgres_db.destination_us_elections
21/01/26 14:17:12 INFO CSVtoJdbcConnector$: showing data in destination table :
+----------+-----+-----------+
|state_code|party|total_votes|
+----------+-----+-----------+
|        MN|  Grn|      13045|
|        AL|  Dem|     793620|
|        ID|  GOP|     420750|
|        ID|  Ind|       2495|
|        WA|  CST|       7772|
|        HI|  Grn|       3121|
|        MS|   RP|        969|
|        NY|  GOP|    2226637|
|        MI|  CST|      16792|
|        ID|  Dem|     212560|
|        AZ|  Lib|      27523|
|        CO|  Dem|    1238490|
|        NY|  Dem|    3875826|
|        PA|  GOP|    2619583|
|        OK|  GOP|     889372|
|        NJ|  Ind|        907|
|        NY|  CST|       6354|
|        AZ|  GOP|    1107130|
|        MO|  CST|       7914|
|        ND|  Grn|       1357|
+----------+-----+-----------+
only showing top 20 rows
```
