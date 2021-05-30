import com.github.edge.roman.spear.SpearConnector
import com.github.edge.roman.spear.connectors.AbstractConnector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.scalatest._

import java.util.Properties

class Test extends FunSuite with BeforeAndAfter {
  val properties = new Properties()
  properties.put("driver", "org.postgresql.Driver")
  properties.put("user", "postgres_user")
  properties.put("password", "mysecretpassword")
  properties.put("url", "jdbc:postgresql://localhost:5432/pgdb")

  SpearConnector.sparkConf.setMaster("local[*]")
  SpearConnector.spark.sparkContext.setLogLevel("ERROR")

  //connector logic
  val csvJdbcConnector: AbstractConnector = SpearConnector
    .createConnector("CSVTOPOSTGRES")
    .source(sourceType = "file", sourceFormat = "csv")
    .target(targetType = "relational", targetFormat = "jdbc")
    .getConnector

  csvJdbcConnector.setVeboseLogging(true)

  csvJdbcConnector
    .source(sourceObject = "data/us-election-2012-results-by-county.csv", Map("header" -> "true", "inferSchema" -> "true"))
    .saveAs("__tmp__")
    .targetJDBC(tableName = "test_table", properties, SaveMode.Overwrite)

  runTests(fileDF("data/us-election-2012-results-by-county.csv"), tableDf("test_table", Map("driver" -> "org.postgresql.Driver", "user" -> "postgres_user", "password" -> "mysecretpassword", "url" -> "jdbc:postgresql://localhost:5432/pgdb")), "csvtopostgresconnector")

  after {
    SpearConnector.spark.stop()
  }


  def fileDF(fileName: String): DataFrame = {
    SpearConnector.spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(fileName)
  }

  def tableDf(tableName: String, params: Map[String, String]): DataFrame = {
    SpearConnector.spark.read.format("jdbc").option("dbtable", tableName).options(params).load()
  }


  def runTests(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testDataComparison(df1, df2, name)
    testCount(df1, df2, name)
  }

  def testDataComparison(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Data Compare Test:$name") {
      assert(diff == 0)
    }
  }

  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = SpearConnector.spark.emptyDataFrame.count()
    test(s"Source destination counts test:$name") {
      assert(count1 == count2)
    }
    test(s"Source dest count mismatch test:$name ") {
      assert(count1 != count3)
    }
  }


  private def compare(df1: DataFrame, df2: DataFrame): Long =
    df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti").count()

  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))

}
