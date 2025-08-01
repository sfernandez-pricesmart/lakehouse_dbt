-- =====================================================================
-- ETL Monitoring Views and Procedures
-- =====================================================================

-- Create a view for ETL execution monitoring
CREATE OR REPLACE VIEW DOMO.WRITEBACK.VW_ETL_EXECUTION_SUMMARY AS
SELECT 
    DATE_TRUNC('DAY', "run_timestamp") AS "execution_date",
    COUNT(*) AS "total_runs",
    SUM(CASE WHEN "status" = 'SUCCESS' THEN 1 ELSE 0 END) AS "successful_runs",
    SUM(CASE WHEN "status" = 'FAILED' THEN 1 ELSE 0 END) AS "failed_runs",
    AVG("execution_time_seconds") AS "avg_execution_time_seconds",
    MAX("execution_time_seconds") AS "max_execution_time_seconds",
    SUM("records_processed") AS "total_records_processed"
FROM DOMO.WRITEBACK."PROD_active_base_history_log"
WHERE "operation_type" IN ('ETL_COMPLETE', 'ETL_ERROR')
GROUP BY DATE_TRUNC('DAY', "run_timestamp")
ORDER BY "execution_date" DESC;

-- Create a view for recent ETL runs
CREATE OR REPLACE VIEW DOMO.WRITEBACK.VW_ETL_RECENT_RUNS AS
SELECT 
    "run_timestamp",
    "operation_type",
    "status",
    "records_processed",
    "execution_time_seconds",
    "error_message"
FROM DOMO.WRITEBACK."PROD_active_base_history_log"
WHERE "run_timestamp" >= DATEADD(DAYS, -7, CURRENT_TIMESTAMP())
ORDER BY "run_timestamp" DESC;

-- Create a view for data quality checks
CREATE OR REPLACE VIEW DOMO.WRITEBACK.VW_ACTIVE_BASE_DATA_QUALITY AS
WITH quality_checks AS (
    SELECT 
        COUNT(*) AS "total_records",
        COUNT(DISTINCT "record_key") AS "unique_records",
        COUNT(CASE WHEN "active_accounts" IS NULL THEN 1 END) AS "null_active_accounts",
        COUNT(CASE WHEN "active_accounts" < 0 THEN 1 END) AS "negative_active_accounts",
        COUNT(CASE WHEN "home_business" IS NULL THEN 1 END) AS "null_home_business",
        COUNT(CASE WHEN "fiscal_year" IS NULL THEN 1 END) AS "null_fiscal_year",
        MIN("last_upd") AS "earliest_update",
        MAX("last_upd") AS "latest_update",
        COUNT(DISTINCT "fiscal_year") AS "distinct_fiscal_years",
        COUNT(DISTINCT "home_business") AS "distinct_stores"
    FROM DOMO.WRITEBACK."PROD_active_base_history"
)
SELECT 
    *,
    CASE 
        WHEN "total_records" = "unique_records" THEN 'PASS'
        ELSE 'FAIL'
    END AS "uniqueness_check",
    CASE 
        WHEN "null_active_accounts" = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS "null_accounts_check",
    CASE 
        WHEN "negative_active_accounts" = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS "negative_accounts_check",
    CURRENT_TIMESTAMP() AS "check_timestamp"
FROM quality_checks;

-- =====================================================================
-- ETL Alerting Procedure
-- =====================================================================
CREATE OR REPLACE PROCEDURE DOMO.WRITEBACK.SP_CHECK_ETL_HEALTH()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    failed_runs INTEGER;
    last_run_time TIMESTAMP;
    hours_since_last_run INTEGER;
    data_quality_issues INTEGER;
    alert_message STRING := '';
BEGIN
    -- Check for failed runs in the last 24 hours
    SELECT COUNT(*)
    INTO failed_runs
    FROM DOMO.WRITEBACK."PROD_active_base_history_log"
    WHERE "status" = 'FAILED' 
    AND "run_timestamp" >= DATEADD(HOURS, -24, CURRENT_TIMESTAMP());
    
    -- Check when was the last successful run
    SELECT MAX("run_timestamp")
    INTO last_run_time
    FROM DOMO.WRITEBACK."PROD_active_base_history_log"
    WHERE "status" = 'SUCCESS';
    
    SELECT DATEDIFF(HOURS, last_run_time, CURRENT_TIMESTAMP())
    INTO hours_since_last_run;
    
    -- Check for data quality issues
    SELECT 
        CASE WHEN "uniqueness_check" = 'FAIL' THEN 1 ELSE 0 END +
        CASE WHEN "null_accounts_check" = 'FAIL' THEN 1 ELSE 0 END +
        CASE WHEN "negative_accounts_check" = 'FAIL' THEN 1 ELSE 0 END
    INTO data_quality_issues
    FROM DOMO.WRITEBACK.VW_ACTIVE_BASE_DATA_QUALITY;
    
    -- Generate alerts
    IF failed_runs > 0 THEN
        alert_message := alert_message || 'ALERT: ' || failed_runs || ' failed ETL runs in the last 24 hours. ';
    END IF;
    
    IF hours_since_last_run > 30 THEN
        alert_message := alert_message || 'ALERT: Last successful ETL run was ' || hours_since_last_run || ' hours ago. ';
    END IF;
    
    IF data_quality_issues > 0 THEN
        alert_message := alert_message || 'ALERT: ' || data_quality_issues || ' data quality issues detected. ';
    END IF;
    
    IF alert_message = '' THEN
        alert_message := 'ETL system is healthy. No issues detected.';
    END IF;
    
    -- Log the health check
    INSERT INTO DOMO.WRITEBACK."PROD_active_base_history_log" 
    ("operation_type", "status", "records_processed", "error_message")
    VALUES ('HEALTH_CHECK', 'COMPLETE', 0, alert_message);
    
    RETURN alert_message;
END;
$$;

-- =====================================================================
-- Data Backup and Recovery Procedure
-- =====================================================================
CREATE OR REPLACE PROCEDURE DOMO.WRITEBACK.SP_BACKUP_ACTIVE_BASE_HISTORY()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    backup_table_name STRING;
    record_count INTEGER;
BEGIN
    -- Create backup table name with timestamp
    backup_table_name := 'PROD_active_base_history_backup_' || 
                        REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING, ' ', '_'), ':', '');
    
    -- Create backup table
    EXECUTE IMMEDIATE 'CREATE TABLE DOMO.WRITEBACK."' || backup_table_name || '" AS 
                       SELECT * FROM DOMO.WRITEBACK."PROD_active_base_history"';
    
    -- Get record count
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM DOMO.WRITEBACK."' || backup_table_name || '"'
    INTO record_count;
    
    -- Log the backup
    INSERT INTO DOMO.WRITEBACK."PROD_active_base_history_log" 
    ("operation_type", "status", "records_processed", "error_message")
    VALUES ('BACKUP_COMPLETE', 'SUCCESS', record_count, 'Backup table: ' || backup_table_name);
    
    RETURN 'Backup completed successfully. Table: ' || backup_table_name || 
           ', Records: ' || record_count;
END;
$$;

-- =====================================================================
-- Utility Queries for Monitoring
-- =====================================================================

-- Query to check current data coverage
/*
SELECT 
    "fiscal_year",
    COUNT(DISTINCT "home_business") AS "stores_count",
    COUNT(*) AS "total_records",
    MIN("last_upd") AS "earliest_update",
    MAX("last_upd") AS "latest_update"
FROM DOMO.WRITEBACK."PROD_active_base_history"
GROUP BY "fiscal_year"
ORDER BY "fiscal_year" DESC;
*/

-- Query to identify potential data gaps
/*
WITH date_spine AS (
    SELECT 
        "fiscal_year",
        "home_business",
        DATEADD(MONTH, seq4(), DATE_TRUNC('YEAR', CURRENT_DATE())) AS "expected_month"
    FROM (SELECT DISTINCT "fiscal_year", "home_business" FROM DOMO.WRITEBACK."PROD_active_base_history")
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 12))
),
actual_data AS (
    SELECT DISTINCT 
        "fiscal_year",
        "home_business",
        DATE_TRUNC('MONTH', TO_DATE("fiscal_year" || '-' || "month" || '-01', 'YYYY-MMMM-DD')) AS "actual_month"
    FROM DOMO.WRITEBACK."PROD_active_base_history"
)
SELECT 
    ds."fiscal_year",
    ds."home_business",
    ds."expected_month"
FROM date_spine ds
LEFT JOIN actual_data ad 
    ON ds."fiscal_year" = ad."fiscal_year" 
    AND ds."home_business" = ad."home_business"
    AND ds."expected_month" = ad."actual_month"
WHERE ad."actual_month" IS NULL
ORDER BY ds."fiscal_year", ds."home_business", ds."expected_month";
*/