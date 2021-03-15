name := "spear-framework"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.antlr" % "stringtemplate" % "4.0",
  "com.databricks" %% "spark-xml" % "0.11.0",
  "org.apache.spark" %% "spark-avro" % "2.4.3",
  "mysql" % "mysql-connector-java" % "8.0.23"
)