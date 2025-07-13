{{ config(
    alias='MASTER_sales_executive_list_file_f57637l',
    materialized='view'
) }}

select 
    trim("SLCLUB") as "Cost_Center",
    trim("SLREGN") as "Company",
    trim("SL$SEN") as "Salesperson_Number",
    trim("SL$NAM") as "Salesperson_Name",
    trim("SLPFLG") as "Process_Flag",
    trim("SLUPGM") as "Update_Program",
    trim("SLUSER") as "User_Id",

    case
        when SLUPMJ = 0 then null
        else dateadd(
            day,
            substring((SLUPMJ + 1900000)::varchar, 5, 3)::number - 1,
            (substring((SLUPMJ + 1900000)::varchar, 1, 4) || '-01-01')::date
        )
    end as "Date_Updated",

    case
        when length(SLTDAY) = 1 then to_time(time_from_parts(0, 0, SLTDAY))
        when length(SLTDAY) = 2 then to_time(time_from_parts(0, 0, SLTDAY))
        when length(SLTDAY) = 3 then to_time(time_from_parts(0, substring(SLTDAY, 1, 1), substring(SLTDAY, 2, 2)))
        when length(SLTDAY) = 4 then to_time(time_from_parts(0, substring(SLTDAY, 1, 2), substring(SLTDAY, 3, 2)))
        when length(SLTDAY) = 5 then to_time(time_from_parts(truncate((SLTDAY / 10000), 0), substring(SLTDAY, 2, 2), substring(SLTDAY, 4, 2)))
        when length(SLTDAY) = 6 then to_time(time_from_parts(truncate((SLTDAY / 10000), 0), substring(SLTDAY, 3, 2), substring(SLTDAY, 5, 2)))
        else to_time(time_from_parts(0, 0, 0))
    end as "Time_Updated",

    trim("SV_PROGRAM_NAME") as SV_PROGRAM_NAME,
    trim("SV_JOB_USER") as SV_JOB_USER,
    convert_timezone('America/Los_Angeles', to_timestamp("SV_OP_TIMESTAMP", 'YYYY-MM-DD HH24:MI:SS.FF6')) as SV_OP_TIMESTAMP

from {{ source('peicust', 'RAW_sales_executive_list_f57637l') }}
