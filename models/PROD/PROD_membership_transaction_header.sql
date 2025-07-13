{{ config(
    alias='PROD_membership_transaction_header',
    materialized='view'
) }}

with preprocessing_memb_header as (

    select
        a."Cost_Center" as "CostCenter",
        a."Account_Number" as "AccountNumber",
        concat(a."Cost_Center", right(concat('000000', a."Account_Number"), 6)) as "AccountNumberFull",
        a."Order_Number" as "OrderNumber",
        a.SV_JOB_USER as "UserId",
        a."Transaction_Date" as "TransactionDate",
        a."Account_Type" as "AccountType",
        a."Complimentary",
        a."Effective_Date" as "EffectiveDate",

        case
            when g."Opening_date" > a."Transaction_Date"
                 and a."Expiration_Date" < dateadd(year, 1, g."Opening_date")
                 and a."Transaction_Type" in ('N', 'P', 'R', '1R', '0R')
            then dateadd(year, 1, g."Opening_date")
            else a."Expiration_Date"
        end as "ExpiredDate",

        a."Currency_Code" as "CurrencyCode",

        case
            when a."Transaction_Type" in ('P', 'X') and b."Company" = '00640' then a."Total_Cost"
            else a."Total_Cost" / 100
        end as "TotalCost",

        case
            when a."Transaction_Type" in ('P', 'X') and b."Company" = '00640' then a."Total_Cost" * f."divider"
            else a."Total_Cost" / 100 * f."divider"
        end as "TotalCostUsd",

        b."Company",
        f."Exchange Rate" as "ExchangeRate",
        a."Payment_Type_Code" as "PaymentTypeCode",
        a."Sales_Order_Number" as "SalesOrderNumber",
        a."Home_Cost_Center" as "HomeCostCenter",
        a."Transaction_Type" as "TransactionType",
        a."Platinum_Transaction" as "PlatinumTransaction",
        a."Invoice_Number" as "InvoiceNumber",
        d."Salesperson_Number" as "SalespersonNumber",
        e."Salesperson_Name" as "Clerk",
        d."Commission_Amt_Local" / 100 as "CommissionAmtLocal",
        d."Receipt_Amt_Local" / 100 as "ReceiptAmtLocal",
        b."Tax_Type" as "TaxType",
        b."Tax_Percent" as "TaxPercent",
        cast(left(nullif(a."Cost_Center", ''), 2) as integer) as "CountryCode"

    from {{ ref('MASTER_membership_account_transaction_header_f55637') }} a
    left join {{ ref('DICT_udc_tax_or_vat_percent') }} b
        on left(a."Home_Cost_Center", 3) = b."Company_Code"
    left join {{ ref('MASTER_sales_executive_membership_commission_transactions_f57637') }} d
        on a."Order_Number" = d."Order_Number" and a."Home_Cost_Center" = d."Transaction_Cost_Center"
    left join {{ ref('MASTER_sales_executive_list_file_f57637l') }} e
        on d."Salesperson_Number" = e."Salesperson_Number" and d."Cost_Center" = e."Cost_Center"
    left join {{ source('domo', 'DICT_exchange_rates') }} f
        on a."Transaction_Date" = f."date" and a."Home_Cost_Center" = f."Warehouse Number"
    left join {{ source('domo','DICT_club_and_country_names') }} g
        on a."Home_Cost_Center" = g."Warehouse_Number"

),

external_tax as (

    select
        p.*,
        e."PERCENTAGE" as "Percentage",
        e."TYPE" as "Type",
        e."US_STYLE" as "UsStyle",
        e."TAX_INCLUDED_POS" as "TaxIncludedPos",
        e."TAX_INCLUDED_DESK" as "TaxIncludedDesk",
        e."DIVIDER_POS" as "DividerPos",
        e."DIVIDER_DESK" as "DividerDesk"

    from preprocessing_memb_header p
    left join {{ ref('DICT_membership_tax_by_country') }} e
        on p."CountryCode" = e."COUNTRY_CODE"

),

tax_adjustments as (

    select
        "TransactionType",
        "EffectiveDate",
        "ExpiredDate",
        "CostCenter",
        "AccountNumber",
        "TransactionDate",
        "AccountNumberFull",
        "TotalCostUsd",
        "PlatinumTransaction",
        "ReceiptAmtLocal",
        "OrderNumber",
        "UserId",
        "AccountType",
        "Complimentary",
        "CurrencyCode",
        "ExchangeRate",
        "PaymentTypeCode",
        "HomeCostCenter",
        "InvoiceNumber",
        "CommissionAmtLocal",
        "TaxPercent",
        "SalespersonNumber",

        case
            when "TransactionType" = 'P' then
                case
                    when "CountryCode" = 64 then
                        case
                            when "TransactionDate" >= '2019-06-30' and "TransactionDate" < '2019-12-08' and "TaxIncludedPos" = 1
                                then "TotalCostUsd" / (1 + 0.13)
                            when "TransactionDate" >= '2024-01-03' and "TaxIncludedPos" = 1
                                then "TotalCostUsd" / (1 + 0.13)
                            else "TotalCostUsd" / (1 + "Percentage")
                        end
                    when "CountryCode" = 61 and "TransactionDate" >= '2022-12-06'
                        then "TotalCostUsd"
                    when "CountryCode" = 66 and "TransactionDate" < '2023-06-20'
                        then "TotalCostUsd"
                    else
                        case
                            when "TaxIncludedPos" = 1 then "TotalCostUsd" / (1 + "Percentage")
                            else "TotalCostUsd"
                        end
                end
            else
                case
                    when "CountryCode" = 64 then
                        case
                            when "TransactionDate" >= '2019-06-30' and "TransactionDate" < '2019-12-08' and "TaxIncludedDesk" = 1
                                then "TotalCostUsd" / (1 + 0.13)
                            when "TransactionDate" >= '2024-01-03' and "TaxIncludedPos" = 1
                                then "TotalCostUsd" / (1 + 0.13)
                            else "TotalCostUsd" / (1 + "Percentage")
                        end
                    when "CountryCode" = 61 and "TransactionDate" >= '2022-12-06'
                        then "TotalCostUsd"
                    when "CountryCode" = 66 and "TransactionDate" < '2023-06-20'
                        then "TotalCostUsd"
                    else
                        case
                            when "TaxIncludedDesk" = 1 then "TotalCostUsd" / (1 + "Percentage")
                            else "TotalCostUsd"
                        end
                end
        end as "TotalUsdCostNet",

        "TotalCost",
        "TaxType",
        "SalesOrderNumber",
        "Clerk",
        "CountryCode"

    from external_tax

)

select
    "CostCenter",
    "AccountNumber",
    "AccountNumberFull",
    "OrderNumber",
    "UserId",
    "TransactionDate",
    "AccountType",
    "Complimentary",
    "EffectiveDate",
    "ExpiredDate",
    "CurrencyCode",
    "TotalCost",
    "TotalCostUsd",
    "ExchangeRate",
    "PaymentTypeCode",
    "SalesOrderNumber",
    "HomeCostCenter",
    "TransactionType",
    "PlatinumTransaction",
    "InvoiceNumber",
    "SalespersonNumber",
    "Clerk",
    "CommissionAmtLocal",
    "ReceiptAmtLocal",
    "TaxType",
    "TaxPercent",
    "CountryCode",
    "TotalUsdCostNet"

from tax_adjustments
