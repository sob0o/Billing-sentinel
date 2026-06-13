ThisBuild / organization := "com.billingsentinel"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "billing-sentinel-spark",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql"            % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "org.apache.hadoop"  % "hadoop-aws"          % "3.3.4",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262",
    ),

    assembly / assemblyOutputPath := file("target/billing-sentinel-assembly.jar"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )