CREATE TABLE playground.{{ var('sandbox_schema') }}."RAW_membership_transaction_header_f55637" CLONE CDC.peicust.f55637;

CREATE TABLE playground.{{ var('sandbox_schema') }}."RAW_sales_executive_membership_commission_transactions_f57637" CLONE CDC.peicust.f57637;

CREATE TABLE playground.{{ var('sandbox_schema') }}."RAW_sales_executive_list_f57637l" CLONE CDC.peicust.F57637L;

CREATE TABLE playground.{{ var('sandbox_schema') }}."RAW_user_defined_codes_f0005" CLONE CDC.peicom.f0005;

CREATE TABLE playground.{{ var('sandbox_schema') }}."DICT_club_and_country_names" CLONE PSMT_LAKEHOUSE_DEV.AS400."DICT_club_and_country_names";

CREATE TABLE playground.{{ var('sandbox_schema') }}."DICT_exchange_rates" CLONE DOMO.WRITEBACK."DICT_exchange_rate";

CREATE TABLE playground.{{ var('sandbox_schema') }}."DICT_membership_tax_by_country" CLONE PSMT_LAKEHOUSE_DEV.MANUAL_FILES.DICT_MEMBERSHIP_TAX_BY_COUNTRY;