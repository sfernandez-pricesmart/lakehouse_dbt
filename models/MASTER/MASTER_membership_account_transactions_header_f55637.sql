{{ config(
    alias='MASTER_membership_account_transactions_header_f55637',
    materialized='view'
) }}

select 
  trim("$HMCU") as "Cost_Center",
  "$H$ACC" as "Account_Number",
  "$HDOCO" as "Order_Number",
  case 
    when "$HTRDJ" = 0 then NULL
    else dateadd(days, substring(("$HTRDJ" + 1900000), 5, 3)::number - 1, (substring(("$HTRDJ" + 1900000), 1, 4) || '-01-01')::date)
  end as "Transaction_Date",
  trim("$H$ATY") as "Account_Type",
  trim("$H$CMP") as "Complimentary",
  case 
    when "$HEFTJ" = 0 then NULL
    else dateadd(days, substring(("$HEFTJ" + 1900000), 5, 3)::number - 1, (substring(("$HEFTJ" + 1900000), 1, 4) || '-01-01')::date)
  end as "Effective_Date",
  case 
    when "$HEXDJ" = 0 then NULL
    else dateadd(days, substring(("$HEXDJ" + 1900000), 5, 3)::number - 1, (substring(("$HEXDJ" + 1900000), 1, 4) || '-01-01')::date)
  end as "Expiration_Date",
  to_number("$HTOTC", 15, 2) as "Total_Cost",
  trim("$HCRRM") as "Mode_(F)",
  trim("$HCRCD") as "Currency_Code",
  "$HCRR" as "Exchange_Rate",
  trim("$HLNGP") as "Language",
  "$HFCST" as "Foreign_Total_Cost",
  trim("$H$PCD") as "Payment_Type_Code",
  trim("$HPRCK") as "Check/Item_Number",
  trim("$HPRFL") as "Processed_Flag",
  "$HDOCM" as "Sales_Order_Number",
  trim("$H$REM") as "Remarks",
  trim("$HUPGM") as "Update_Program",
  trim("$HHMCU") as "Home_Cost_Center",
  trim("$H$C01") as "Transaction_Type",
  trim("$H$C02") as "Platinum_Transaction",
  trim("$H$C03") as "Category_Code_03",
  trim("$H$C04") as "Category_Code_04",
  trim("$H$C05") as "Category_Code_05",
  "$HDOCZ" as "Invoice_Number",
  trim("SV_PROGRAM_NAME") as "SV_PROGRAM_NAME",
  trim("SV_JOB_USER") as "SV_JOB_USER",
  convert_timezone('America/Los_Angeles', to_timestamp("SV_OP_TIMESTAMP", 'YYYY-MM-DD HH24:MI:SS.FF6')) as "SV_OP_TIMESTAMP"
from {{ source('peicust', 'RAW_membership_transaction_header_f55637') }}
