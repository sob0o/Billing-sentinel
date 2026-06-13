// ThisBuild settings apply to all sub-projects (there's only one here)
ThisBuild / organization := "com.billingsentinel"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"  // Must match Spark 3.5.1's Scala version

lazy val root = (project in file("."))
  .settings(
    name := "billing-sentinel-spark",

    libraryDependencies ++= Seq(
      // Spark core — "provided" means Spark cluster already has it, don't bundle it
      "org.apache.spark" %% "spark-core"           % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql"            % "3.5.1" % "provided",

      // Kafka connector — NOT provided, must be bundled in JAR
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",

      // S3/MinIO support
      "org.apache.hadoop"  % "hadoop-aws"          % "3.3.4",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262",

      // ✅ NEW: PostgreSQL JDBC driver — needed to write to PostgreSQL
      "org.postgresql"     % "postgresql"          % "42.7.3",
    ),

    // Where the fat JAR is written
    assembly / assemblyOutputPath := file("target/billing-sentinel-assembly.jar"),

    // How to handle conflicts when merging JARs
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )