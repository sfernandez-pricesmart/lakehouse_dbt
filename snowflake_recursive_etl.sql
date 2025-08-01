-- =====================================================================
-- Recursive ETL for PROD_active_base_history
-- This procedure handles delta generation and historical data updates
-- =====================================================================

-- First, create the historical table if it doesn't exist
CREATE TABLE IF NOT EXISTS DOMO.WRITEBACK."PROD_active_base_history" (
    "record_key" VARCHAR(50),
    "period" VARCHAR(50),
    "fiscal_year" INTEGER,
    "month" VARCHAR(20),
    "home_business" VARCHAR(20),
    "active_accounts" INTEGER,
    "Country Store Name" VARCHAR(100),
    "Country Name English" VARCHAR(100),
    "Country name Spanish" VARCHAR(100),
    "Region" VARCHAR(50),
    "Region Extended" VARCHAR(100),
    "Country Code" VARCHAR(10),
    "isActiveClub" VARCHAR(10),
    "last_upd" DATE,
    PRIMARY KEY ("record_key")
);

-- Create a logging table for ETL monitoring
CREATE TABLE IF NOT EXISTS DOMO.WRITEBACK."PROD_active_base_history_log" (
    "log_id" INTEGER AUTOINCREMENT,
    "run_timestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    "operation_type" VARCHAR(50),
    "records_processed" INTEGER,
    "status" VARCHAR(20),
    "error_message" VARCHAR(1000),
    "execution_time_seconds" INTEGER
);

-- =====================================================================
-- Main ETL Stored Procedure
-- =====================================================================
CREATE OR REPLACE PROCEDURE DOMO.WRITEBACK.SP_REFRESH_PROD_ACTIVE_BASE_HISTORY()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    execution_time INTEGER;
    records_inserted INTEGER := 0;
    records_updated INTEGER := 0;
    error_msg STRING := '';
    result_msg STRING := '';
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    -- Log the start of the process
    INSERT INTO DOMO.WRITEBACK."PROD_active_base_history_log" 
    ("operation_type", "status", "records_processed")
    VALUES ('ETL_START', 'RUNNING', 0);
    
    BEGIN
        -- =====================================================================
        -- STEP 1: Generate Delta Data
        -- =====================================================================
        
        -- Create temporary table for delta calculation
        CREATE OR REPLACE TEMPORARY TABLE temp_delta_active_base AS
        WITH
            -- Date logic for period determination
            date_converted AS ( 
                SELECT   
                    TO_DATE(
                        CONCAT("fiscal_year", '-', "month", '-01'),
                        'YYYY-MMMM-DD'
                    ) AS date_converted,
                    TO_DATE(
                        CONCAT("fiscal_year", '-September-01'),
                        'YYYY-MMMM-DD'
                    ) AS date_limit
                FROM DOMO.WRITEBACK."PROD_active_base_history"
                WHERE "last_upd" IS NOT NULL
            ),
            
            date_limit AS (  
                SELECT COALESCE(MAX(date_limit), DATE('1900-01-01')) AS "MAX_DATE_LIMIT"
                FROM date_converted
            ),
            
            date_conditioned AS ( 
                SELECT COALESCE(MAX(d.date_converted), DATE('1900-01-01')) AS "MAX_DATE_CONVERTED_CON"
                FROM date_converted d
                JOIN date_limit l ON TRUE
                WHERE d.date_converted < l."MAX_DATE_LIMIT"
            ),
            
            date_unconditioned AS ( 
                SELECT COALESCE(MAX(d.date_converted), DATE('1900-01-01')) AS "MAX_DATE_CONVERTED_UNC"
                FROM date_converted d
            ),
            
            comp AS(  
                SELECT d.*, e.*,
                CAST(MONTH(DATE(d."MAX_DATE_CONVERTED_CON")) AS INTEGER) AS "month_in_numeric"
                FROM date_conditioned d
                CROSS JOIN date_unconditioned e
            ),
            
            condition AS(  
                SELECT
                CASE WHEN (c."month_in_numeric" < 9) THEN c."MAX_DATE_CONVERTED_CON" ELSE c."MAX_DATE_CONVERTED_UNC" END AS "PERIOD"
                FROM comp c
            ),
            
            condition_plus_month AS ( 
                SELECT
                    DATEADD(MONTH, 1, "PERIOD") AS "PERIOD_PLUS_MONTH"
                FROM condition
            ),
            
            period_condition AS (  
                SELECT
                    CASE 
                        WHEN CURRENT_DATE = MAX(ac."last_upd") OVER() THEN c."PERIOD"
                        ELSE cp."PERIOD_PLUS_MONTH"
                    END AS "PERIOD_TO_MONTH"
                FROM condition c
                CROSS JOIN condition_plus_month cp     
                LEFT JOIN DOMO.WRITEBACK."PROD_active_base_history" ac 
                    ON c."PERIOD" = ac."period"
            ),
            
            -- Current period calculation
            pcperiod AS ( 
                SELECT
                    MIN(ca."PCDATE") AS "MINPCDATE",
                    MAX(ca."PCDATE") AS "MAXPCDATE",
                    "PCFYR",
                    "PCPER",
                    TO_DATE(DATEADD(HOUR, -24, CURRENT_TIMESTAMP())) AS "DAY"
                FROM PSMT_LAKEHOUSE_DEV.AS400."DICT_calendar_helper" ca
                GROUP BY "PCFYR", "PCPER"
            ),
            
            period_ind AS (  
                SELECT *,
                    CASE 
                        WHEN "DAY" >= "MINPCDATE" AND "DAY" <= "MAXPCDATE" THEN 1
                        ELSE 0
                    END AS "PERIOD_IND"
                FROM pcperiod
            ),
            
            period_ind_filtered AS (   
                SELECT 
                    p."MINPCDATE",
                    p."MAXPCDATE",
                    CASE 
                        WHEN MONTH("MINPCDATE") <= 8 THEN (p."PCFYR" - 1)
                        ELSE p."PCFYR"
                    END AS "PCFYR",
                    CASE 
                        WHEN MONTH("MINPCDATE") <= 8 THEN (p."PCPER" + 11)
                        ELSE p."PCPER"
                    END AS "PCPER",
                    p."DAY",
                    p."PERIOD_IND"
                FROM period_ind p
                WHERE p."PERIOD_IND" = 1
            ),
            
            -- Membership data processing
            membership AS (  
                SELECT *,
                    TO_DATE(DATEADD(HOUR, -24, CURRENT_TIMESTAMP())) AS "DAY",
                    CASE
                        WHEN pacc."Cancel_Date" IS NULL THEN pacc."Expiration_Date"
                        ELSE pacc."Cancel_Date"
                    END AS "COMP_DATE"
                FROM PSMT_LAKEHOUSE_DEV.AS400."PROD_membership_accounts" pacc
                WHERE pacc."Account_Type" <> 'EM'  -- Exclude Employee memberships
            ),
            
            base_membership_with_date AS( 
                SELECT 
                    i."MAXPCDATE",
                    i."PCFYR",
                    m.*
                FROM period_ind_filtered i
                LEFT JOIN membership m 
                ON i."DAY" = m."DAY"
            ),
            
            preview_indicator_active AS (  
                SELECT *,
                    CASE 
                        WHEN h."COMP_DATE" >= h."DAY" THEN 1
                        ELSE 0
                    END AS "indicator_active"
                FROM base_membership_with_date h
            ),
            
            -- Membership refund logic
            membership_refund AS (
                WITH ranked AS (
                    SELECT
                        "AccountNumberFull",
                        "TransactionDate",
                        "TransactionType",
                        ROW_NUMBER() OVER (
                            PARTITION BY "AccountNumberFull"
                            ORDER BY "TransactionDate" DESC
                        ) AS "RankByAccount"
                    FROM ELYSIUM_DEV."Memberships"."FactMembershipsTransactions"
                )
                SELECT DISTINCT
                    "AccountNumberFull"
                FROM ranked
                WHERE "RankByAccount" = 1
                    AND "TransactionType" = 'X'
            ),

            preview_indicator_active_filter AS ( 
                SELECT p.*
                FROM preview_indicator_active p
                LEFT JOIN membership_refund c ON p."AccountNumberFull" = c."AccountNumberFull"
                WHERE c."AccountNumberFull" IS NULL
            ),
            
            -- Block status filtering
            block_status_99 AS (
                SELECT DISTINCT
                    LEFT("MembershipNumber", 10) AS "AccountNumberFull"
                FROM ELYSIUM_DEV."Memberships"."DimMembershipsCards"
                WHERE "BlockStatus" = '99'
            ),
            
            indicator_active AS (
                SELECT p.*
                FROM preview_indicator_active_filter p
                LEFT JOIN block_status_99 d ON p."AccountNumberFull" = d."AccountNumberFull" 
                WHERE d."AccountNumberFull" IS NULL
            ),
            
            -- Final aggregation
            filter_by_indicator_active AS (
                SELECT 
                    act."DAY" AS "today",
                    LAST_DAY(DATEADD(HOUR, -31, CURRENT_TIMESTAMP())) AS "last_day",
                    act."PCFYR" AS "fiscal_year",
                    TRIM(act."Home_Business_Unit") AS "home_business", 
                    COUNT(act."Account_Number") AS "account"
                FROM indicator_active act
                WHERE act."indicator_active" = 1
                GROUP BY act."DAY", act."PCFYR", act."Home_Business_Unit"
            ),
            
            table_to_output AS (
                SELECT tr.*, dic.*
                FROM filter_by_indicator_active tr
                LEFT JOIN PSMT_LAKEHOUSE_DEV.AS400."DICT_club_and_country_names" dic 
                    ON dic."Warehouse_Number" = tr."home_business"
            )
            
        -- Final delta selection
        SELECT
            CONCAT("home_business", '-', ca."fiscal_year", '-', LPAD(MONTH(ca."last_day")::STRING,2,'0')) AS "record_key",
            CONCAT(MONTHNAME(ca."last_day"), ', ', ca."fiscal_year") AS "period",
            ca."fiscal_year",
            TO_VARCHAR(ca."last_day", 'MMMM') AS "month",
            ca."home_business",
            ca."account" AS "active_accounts",
            ca."Country_Store_Name" AS "Country Store Name",
            ca."Country_Name_English" AS "Country Name English",
            ca."Country_Name_Spanish" AS "Country name Spanish",
            ca."Region",
            ca."Region_Extended" AS "Region Extended",
            ca."Country_Code" AS "Country Code",
            ca."isActiveClub",
            CURRENT_DATE() AS "last_upd"
        FROM table_to_output ca 
        WHERE ca."today" = ca."last_day";
        
        -- =====================================================================
        -- STEP 2: Merge Delta with Historical Data
        -- =====================================================================
        
        MERGE INTO DOMO.WRITEBACK."PROD_active_base_history" AS target
        USING temp_delta_active_base AS source
        ON target."record_key" = source."record_key"
        WHEN MATCHED THEN 
            UPDATE SET
                "period" = source."period",
                "fiscal_year" = source."fiscal_year",
                "month" = source."month",
                "home_business" = source."home_business",
                "active_accounts" = source."active_accounts",
                "Country Store Name" = source."Country Store Name",
                "Country Name English" = source."Country Name English",
                "Country name Spanish" = source."Country name Spanish",
                "Region" = source."Region",
                "Region Extended" = source."Region Extended",
                "Country Code" = source."Country Code",
                "isActiveClub" = source."isActiveClub",
                "last_upd" = source."last_upd"
        WHEN NOT MATCHED THEN 
            INSERT (
                "record_key", "period", "fiscal_year", "month", "home_business",
                "active_accounts", "Country Store Name", "Country Name English",
                "Country name Spanish", "Region", "Region Extended", "Country Code",
                "isActiveClub", "last_upd"
            )
            VALUES (
                source."record_key", source."period", source."fiscal_year", source."month",
                source."home_business", source."active_accounts", source."Country Store Name",
                source."Country Name English", source."Country name Spanish", source."Region",
                source."Region Extended", source."Country Code", source."isActiveClub",
                source."last_upd"
            );
            
        -- Get merge statistics
        SELECT 
            $number_of_rows_inserted,
            $number_of_rows_updated
        INTO records_inserted, records_updated;
        
        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF(SECOND, start_time, end_time);
        
        result_msg := 'ETL completed successfully. Records inserted: ' || records_inserted || 
                     ', Records updated: ' || records_updated || 
                     ', Execution time: ' || execution_time || ' seconds';
        
        -- Log successful completion
        INSERT INTO DOMO.WRITEBACK."PROD_active_base_history_log" 
        ("operation_type", "status", "records_processed", "execution_time_seconds")
        VALUES ('ETL_COMPLETE', 'SUCCESS', records_inserted + records_updated, execution_time);
        
    EXCEPTION
        WHEN OTHER THEN
            error_msg := SQLERRM;
            end_time := CURRENT_TIMESTAMP();
            execution_time := DATEDIFF(SECOND, start_time, end_time);
            
            -- Log error
            INSERT INTO DOMO.WRITEBACK."PROD_active_base_history_log" 
            ("operation_type", "status", "records_processed", "error_message", "execution_time_seconds")
            VALUES ('ETL_ERROR', 'FAILED', 0, error_msg, execution_time);
            
            result_msg := 'ETL failed with error: ' || error_msg;
            RAISE;
    END;
    
    RETURN result_msg;
END;
$$;