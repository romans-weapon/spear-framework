ThisBuild / name := "spear-framework"

lazy val scala212 = "2.12.12"
lazy val scala211 = "2.11.12"

val sparkVersion = "2.4.7"

ThisBuild / scalaVersion := scala211

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.11.0",

  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.1017",
  "com.hierynomus" % "smbj" % "0.9.1",
  "jcifs" % "jcifs" % "1.3.17",
  "com.google.cloud" % "google-cloud-storage" % "1.114.0",
  "com.google.cloud" % "google-cloud-nio" % "0.30.0-alpha",
  "com.microsoft.azure" % "azure-storage" % "4.0.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",

  "org.postgresql" % "postgresql" % "42.2.5" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assemblyJarName in assembly := "spear-framework-1.0.jar"

ThisBuild / developers := List(
  Developer(
    id    = "anudeep",
    name  = "Anudeep Konaboina",
    email = "krantianudeep@gmail.com",
    url   = url("https://github.com/AnudeepKonaboina/spear-framework")
  )
)

ThisBuild / description := "Rapid ETL-connectors/pipeline development with minimal code leveraged on top of Apache Spark"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/AnudeepKonaboina/spear-framework"))
