{{ config(
    alias='MASTER_sales_executive_membership_commission_transactions_f57637',
    materialized='view'
) }}

select
    trim("SEMCU") as "Cost_Center",
    "SE$ACC" as "Account_Number",
    "SEDOCO" as "Order_Number",

    case
        when SETRDJ = 0 then null
        else dateadd(
            day,
            substring((SETRDJ + 1900000)::varchar, 5, 3)::number - 1,
            (substring((SETRDJ + 1900000)::varchar, 1, 4) || '-01-01')::date
        )
    end as "Transaction_Date",

    trim("SE$SEN") as "Salesperson_Number",
    trim("SE$ATY") as "Account_Type",
    SETOTC as "Commission_Amt_Local",
    SEFCST as "Comm_Amt_USD",
    SETOTS as "Receipt_Amt_Local",
    trim("SECRRM") as "Mode_(F)",
    trim("SECRCD") as "Currency_Code",
    SECRR as "Exchange_Rate",
    trim("SEPAYM") as "Payment_Method",
    trim("SEAMCU") as "Transaction_Cost_Center",
    trim("SE$C01") as "Transaction_Type",
    trim("SE$C02") as "Platinum_Transaction",
    SEDOCZ as "Invoice",
    trim("SEUPGM") as "Update_Program",
    trim("SEUSER") as "User_ID",

    case
        when SEUPMJ = 0 then null
        else dateadd(
            day,
            substring((SEUPMJ + 1900000)::varchar, 5, 3)::number - 1,
            (substring((SEUPMJ + 1900000)::varchar, 1, 4) || '-01-01')::date
        )
    end as "Date_Updated",

    case
        when length(SETDAY) = 1 then to_time(time_from_parts(0, 0, SETDAY))
        when length(SETDAY) = 2 then to_time(time_from_parts(0, 0, SETDAY))
        when length(SETDAY) = 3 then to_time(time_from_parts(0, substring(SETDAY, 1, 1), substring(SETDAY, 2, 2)))
        when length(SETDAY) = 4 then to_time(time_from_parts(0, substring(SETDAY, 1, 2), substring(SETDAY, 3, 2)))
        when length(SETDAY) = 5 then to_time(time_from_parts(truncate((SETDAY / 10000), 0), substring(SETDAY, 2, 2), substring(SETDAY, 4, 2)))
        when length(SETDAY) = 6 then to_time(time_from_parts(truncate((SETDAY / 10000), 0), substring(SETDAY, 3, 2), substring(SETDAY, 5, 2)))
        else to_time(time_from_parts(0, 0, 0))
    end as "Time_of_Day",

    trim("SECOUN") as "Country_of_Purchase",
    trim("SV_PROGRAM_NAME") as SV_PROGRAM_NAME,
    trim("SV_JOB_USER") as SV_JOB_USER,
    convert_timezone('America/Los_Angeles', to_timestamp("SV_OP_TIMESTAMP", 'YYYY-MM-DD HH24:MI:SS.FF6')) as SV_OP_TIMESTAMP

from {{ source('peicust', 'RAW_sales_executive_membership_commission_transactions_f57637') }}
