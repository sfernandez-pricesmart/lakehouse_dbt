{{ config(
    alias='MASTER_user_defined_codes_f0005',
    materialized='view'
) }}

select
  trim("DRSY") as "UDC_System",
  trim("DRRT") as "UDC_Type",
  trim("DRKY") as "UDC_Code",
  trim("DRDL01") as "UDC_Description1",
  trim("DRDL02") as "UDC_Description2",
  trim("DRSPHD") as "UDC_Special_Handling",
  trim("DRUDCO") as "Ownership_Flag",
  trim("DRHRDC") as "Hard_Coded_Y/N",
  trim("DRUSER") as "User_ID",
  trim("DRPID") as "Program_ID",

  case 
    when DRUPMJ = 0 then null
    else dateadd(
      day, 
      substring((DRUPMJ + 1900000)::varchar, 5, 3)::number - 1,
      (substring((DRUPMJ + 1900000)::varchar, 1, 4) || '-01-01')::date
    )
  end as "Date_Updated",

  trim("DRJOBN") as "Work_Station_ID",

  case
    when length(DRUPMT) = 1 then to_time(time_from_parts(0, 0, DRUPMT))
    when length(DRUPMT) = 2 then to_time(time_from_parts(0, 0, DRUPMT))
    when length(DRUPMT) = 3 then to_time(time_from_parts(0, substring(DRUPMT, 1, 1), substring(DRUPMT, 2, 2)))
    when length(DRUPMT) = 4 then to_time(time_from_parts(0, substring(DRUPMT, 1, 2), substring(DRUPMT, 3, 2)))
    when length(DRUPMT) = 5 then to_time(time_from_parts(truncate((DRUPMT / 10000), 0), substring(DRUPMT, 2, 2), substring(DRUPMT, 4, 2)))
    when length(DRUPMT) = 6 then to_time(time_from_parts(truncate((DRUPMT / 10000), 0), substring(DRUPMT, 3, 2), substring(DRUPMT, 5, 2)))
    else to_time(time_from_parts(0, 0, 0))
  end as "Time_Last_Updated",

  trim("SV_PROGRAM_NAME") as "SV_PROGRAM_NAME",
  trim("SV_JOB_USER") as "SV_JOB_USER",
  convert_timezone('America/Los_Angeles', to_timestamp("SV_OP_TIMESTAMP", 'YYYY-MM-DD HH24:MI:SS.FF6')) as "SV_OP_TIMESTAMP"

from {{ source('peicom', 'RAW_user_defined_codes_f0005') }}
