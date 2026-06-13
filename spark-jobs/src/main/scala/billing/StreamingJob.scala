package billing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingJob {
  def main(args: Array[String]): Unit = {

    // Step 1 - Create Spark session
    val spark = SparkSession.builder()
      .appName("BillingSentinel-Streaming")
      .getOrCreate()

    import spark.implicits._

    println("Billing Sentinel started!")

    // Step 2 - Read from Kafka
    val invoicesDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "invoices")
      .option("startingOffsets", "latest")
      .load()

    println("Reading from Kafka topic: invoices")

    // Step 3 - Define schema
    val schema = new StructType()
      .add("invoice_id",       StringType)
      .add("intervention_id",  StringType)
      .add("subcontractor_id", StringType)
      .add("billed_amount",    DoubleType)
      .add("expected_amount",  DoubleType)
      .add("billed_duration",  DoubleType)
      .add("real_duration",    DoubleType)
      .add("rate_applied",     DoubleType)
      .add("contract_rate",    DoubleType)
      .add("billing_date",     StringType)
      .add("anomaly_type",     StringType)

    // Step 4 - Parse JSON
    val parsedDF = invoicesDF
      .selectExpr("CAST(value AS STRING) as json_string")
      .select(from_json($"json_string", schema).as("data"))
      .select("data.*")

    // Step 5 - Detect anomalies
    val scoredDF = parsedDF
      .withColumn("is_overbilling", ($"billed_duration" > $"real_duration" * 1.15))
      .withColumn("is_wrong_rate",  ($"rate_applied" > $"contract_rate" * 1.10))
      .withColumn("anomaly_score",
        when($"is_overbilling" && $"is_wrong_rate", 1.0)
        .when($"is_overbilling" || $"is_wrong_rate", 0.5)
        .otherwise(0.0)
      )

    // Step 6 - Write to MinIO
    scoredDF.writeStream
      .format("parquet")
      .option("path", "s3a://billing-sentinel/raw/")
      .option("checkpointLocation", "s3a://billing-sentinel/checkpoints/")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}