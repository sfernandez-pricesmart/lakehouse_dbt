-- =====================================================================
-- Snowflake Task for Automated ETL Scheduling
-- =====================================================================

-- Create a warehouse for the ETL process if needed
CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create the task to run the ETL process daily
CREATE OR REPLACE TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY
    WAREHOUSE = ETL_WAREHOUSE
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- Daily at 6 AM EST
    USER_TASK_TIMEOUT_MS = 1800000  -- 30 minutes timeout
    COMMENT = 'Daily refresh of PROD_active_base_history table'
AS
    CALL DOMO.WRITEBACK.SP_REFRESH_PROD_ACTIVE_BASE_HISTORY();

-- Alternative: Create a task that runs after specific conditions
CREATE OR REPLACE TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_CONDITIONAL
    WAREHOUSE = ETL_WAREHOUSE
    SCHEDULE = 'USING CRON 0 */6 * * * America/New_York'  -- Every 6 hours
    USER_TASK_TIMEOUT_MS = 1800000
    COMMENT = 'Conditional refresh of PROD_active_base_history table'
    WHEN SYSTEM$STREAM_HAS_DATA('MEMBERSHIP_CHANGES_STREAM')  -- Only if there are changes
AS
    CALL DOMO.WRITEBACK.SP_REFRESH_PROD_ACTIVE_BASE_HISTORY();

-- Create a task for end-of-month processing
CREATE OR REPLACE TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_MONTH_END
    WAREHOUSE = ETL_WAREHOUSE
    SCHEDULE = 'USING CRON 0 2 L * * America/New_York'  -- Last day of month at 2 AM
    USER_TASK_TIMEOUT_MS = 3600000  -- 60 minutes timeout for month-end
    COMMENT = 'Month-end refresh of PROD_active_base_history table'
AS
    CALL DOMO.WRITEBACK.SP_REFRESH_PROD_ACTIVE_BASE_HISTORY();

-- =====================================================================
-- Task Management Commands
-- =====================================================================

-- Start the main daily task
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY RESUME;

-- Start the conditional task (comment out the daily one if using this)
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_CONDITIONAL RESUME;

-- Start the month-end task
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_MONTH_END RESUME;

-- Monitor task execution
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
-- WHERE NAME = 'TASK_REFRESH_ACTIVE_BASE_HISTORY' 
-- ORDER BY SCHEDULED_TIME DESC;

-- Suspend tasks if needed
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY SUSPEND;
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_CONDITIONAL SUSPEND;
-- ALTER TASK DOMO.WRITEBACK.TASK_REFRESH_ACTIVE_BASE_HISTORY_MONTH_END SUSPEND;