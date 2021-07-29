ThisBuild / organizationName := "Romans Weapon"
ThisBuild / organization := "io.github.romans-weapon"
ThisBuild / organizationHomepage := Some(url("https://github.com/romans-weapon"))
ThisBuild / name := "spear-framework"

//spear-framework version 1.0 for spark 3.1.1
ThisBuild / version := "3.1.1-2.0"

lazy val scala212 = "2.12.10"

val sparkVersion = "3.1.1"

ThisBuild / scalaVersion := scala212

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion% "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion,

  "com.databricks" %% "spark-xml" % "0.12.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.1017",
  "com.hierynomus" % "smbj" % "0.9.1",
  "jcifs" % "jcifs" % "1.3.17",
  "com.google.cloud" % "google-cloud-storage" % "1.114.0",
  "com.google.cloud" % "google-cloud-nio" % "0.30.0-alpha",
  "com.microsoft.azure" % "azure-storage" % "4.0.0",

  "org.postgresql" % "postgresql" % "42.2.5" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
assemblyJarName in assembly := "spear-framework-1.0.jar"

resolvers +=
  "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/service/local/repositories/releases/content"

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/romans-weapon/spear-framework"),
    "scm:git@github.com:romans-weapon/spear-framework.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "anudeep",
    name  = "Anudeep Konaboina",
    email = "krantianudeep@gmail.com",
    url   = url("https://github.com/romans-weapon")
  )
)

ThisBuild / description := "Rapid ETL-connectors/pipeline development with minimal code leveraged on top of Apache Spark"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/romans-weapon/spear-framework"))

// Remove all additional repository other than Maven Central from POM
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
updateOptions := updateOptions.value.withGigahorse(false)
