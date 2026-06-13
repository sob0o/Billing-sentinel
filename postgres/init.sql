-- This file runs automatically when the PostgreSQL container first starts.
-- It creates the two tables our batch job will write to.

-- Table 1: KPIs aggregated per subcontractor
-- Each row = one subcontractor's summary for one batch run
CREATE TABLE IF NOT EXISTS subcontractor_kpis (
    subcontractor_id    VARCHAR(50),
    total_invoices      BIGINT,         -- how many invoices total
    total_billed        DOUBLE PRECISION, -- sum of all billed amounts
    total_expected      DOUBLE PRECISION, -- sum of all expected amounts
    avg_anomaly_score   DOUBLE PRECISION, -- average anomaly score (0.0 to 1.0)
    anomaly_count       BIGINT,         -- how many invoices had anomaly_score > 0
    overbilling_count   BIGINT,         -- how many is_overbilling = true
    wrong_rate_count    BIGINT,         -- how many is_wrong_rate = true
    batch_timestamp     TIMESTAMP DEFAULT NOW() -- when this batch ran
);

-- Table 2: Detected duplicate invoices
-- Each row = one intervention_id that was billed more than once
CREATE TABLE IF NOT EXISTS duplicate_invoices (
    intervention_id     VARCHAR(50),
    invoice_count       BIGINT,         -- how many times it was billed
    subcontractor_ids   TEXT,           -- who billed it (comma-separated)
    total_billed        DOUBLE PRECISION,
    batch_timestamp     TIMESTAMP DEFAULT NOW()
);