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
  .createConnector("CSVTOPOSTGRES")
  .source(sourceType = "file", sourceFormat = "csv")
  .multiTarget
  .getConnector

csvMultiTargetConnector.setVeboseLogging(true)

csvMultiTargetConnector
  .source(sourceObject = "file:///opt/spear-framework/data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
  .saveAs("_table_")
  .branch
  .targets(
    csvMultiTargetConnector.targetJDBC(objectName = "mytable", destFormat = "jdbc", properties, SaveMode.Overwrite),
    csvMultiTargetConnector.transformSql(
      """select state_code,party,
        |sum(votes) as total_votes
        |from _table_
        |group by state_code,party""".stripMargin)
      .targetFS(destinationFilePath = "tmp/ingest/transformed_new", destFormat = "parquet", saveAsTable = "ingest.transformed_new"),
    csvMultiTargetConnector.targetFS(destinationFilePath = "/tmp/ingest/raw_new", destFormat = "parquet", saveAsTable = "ingest.raw_new", saveMode = SaveMode.Overwrite),
    csvMultiTargetConnector.targetNoSQL(objectName = "ingest.cassandra_data_mongo", destFormat = "mongo", props = mongoProps, SaveMode.Overwrite)
  )