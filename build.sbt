ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "connectionpro1"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-hive" % "3.3.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.2")
