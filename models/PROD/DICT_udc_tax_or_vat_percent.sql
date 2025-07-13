{{ config(
    alias='DICT_udc_tax_or_vat_percent',
    materialized='view'
) }}

with tax_type as (
    -- Tipos de impuestos por compañía (TV: Tax Type)
    select
        "UDC_System",
        "UDC_Type",
        concat('00', trim("UDC_Code")) as Company,
        "UDC_Description1" as Tax_Type,
        "UDC_Description2",
        "UDC_Special_Handling"
    from {{ ref('MASTER_user_defined_codes_f0005') }}
    where
        "UDC_System" = '55'
        and "UDC_Type" = 'TV'
),

tax_percent as (
    -- Porcentajes de impuestos por compañía (TX: Tax Percent)
    select
        "UDC_System",
        "UDC_Type",
        concat('00', substr("UDC_Code", 2, 3)) as Company,
        "UDC_Description1",
        cast(substr("UDC_Description2", 1, 5) as decimal(4,2)) as Tax_Percent,
        "UDC_Special_Handling"
    from {{ ref('MASTER_user_defined_codes_f0005') }}
    where
        "UDC_System" = '56'
        and "UDC_Type" = 'TX'
        and "UDC_Code" != '0000'
)

select
    tt.Company as "Company",
    substr(tt.Company, 3, 3) as "Company_Code",
    substr(tt.Company, 3, 2) as "Company_Code_2D",
    tt.Tax_Type as "Tax_Type",
    tp.Tax_Percent as "Tax_Percent"
from tax_type tt
left join tax_percent tp
    on tt.Company = tp.Company


