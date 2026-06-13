package billing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchJob {
  def main(args: Array[String]): Unit = {

    // ── 1. Start Spark ───────────────────────────────────────────────────────
    // Same as StreamingJob but with a different app name.
    // S3/MinIO config is passed via --conf flags in spark-submit, not here.
    val spark = SparkSession.builder()
      .appName("BillingSentinel-Batch")
      .getOrCreate()

    import spark.implicits._

    println("=== Batch Job started ===")

    // ── 2. Read Parquet files from MinIO ─────────────────────────────────────
    // spark.read (not readStream) — this is batch, not streaming.
    // Spark reads ALL .parquet files under this path at once.
    val rawDF = spark.read
      .parquet("s3a://billing-sentinel/raw/")

    println(s"Records found: ${rawDF.count()}")

    // ── 3. KPIs per subcontractor ─────────────────────────────────────────────
    // groupBy groups all rows with the same subcontractor_id together.
    // agg then computes one summary row per group.
    val kpisDF = rawDF
      .groupBy("subcontractor_id")
      .agg(
        count("invoice_id")                                   .as("total_invoices"),
        sum("billed_amount")                                  .as("total_billed"),
        sum("expected_amount")                                .as("total_expected"),
        avg("anomaly_score")                                  .as("avg_anomaly_score"),
        sum(when($"anomaly_score" > 0.0, 1).otherwise(0))    .as("anomaly_count"),
        sum(when($"is_overbilling" === true, 1).otherwise(0)).as("overbilling_count"),
        sum(when($"is_wrong_rate"  === true, 1).otherwise(0)).as("wrong_rate_count")
      )

    // ── 4. Detect duplicate invoices ─────────────────────────────────────────
    // A duplicate = same intervention_id appearing in more than one invoice.
    // We group by intervention_id and count how many invoice rows exist per group.
    // Then we keep only groups where that count is greater than 1.
    val duplicatesDF = rawDF
      .groupBy("intervention_id")
      .agg(
        count("invoice_id")                          .as("invoice_count"),
        collect_set("subcontractor_id")              .as("subcontractor_ids_array"),
        sum("billed_amount")                         .as("total_billed")
      )
      .filter($"invoice_count" > 1)
      // collect_set gives us an array — convert it to a comma-separated string
      // so it's easy to store in PostgreSQL as plain text
      .withColumn("subcontractor_ids", concat_ws(",", $"subcontractor_ids_array"))
      .drop("subcontractor_ids_array")

    println(s"Duplicates found: ${duplicatesDF.count()}")

    // ── 5. Write to PostgreSQL ────────────────────────────────────────────────
    // JDBC connection settings — same for both tables
    val jdbcUrl  = "jdbc:postgresql://postgres:5432/billing_sentinel"
    val jdbcProps = new java.util.Properties()
    jdbcProps.setProperty("user",   "sentinel")
    jdbcProps.setProperty("password", "sentinel")
    jdbcProps.setProperty("driver", "org.postgresql.Driver")

    // "overwrite" means: on each batch run, replace the table contents.
    // This way the dashboard always shows fresh results, not accumulating duplicates.
    kpisDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "subcontractor_kpis", jdbcProps)

    duplicatesDF.write
      .mode("overwrite")
      .jdbc(jdbcUrl, "duplicate_invoices", jdbcProps)

    println("=== Batch Job complete ===")

    spark.stop()
  }
}