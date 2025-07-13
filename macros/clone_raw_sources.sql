{% macro clone_raw_sources() %}
    {% set schema = env_var('SANDBOX_SCHEMA') %}

    {% set clone_statements = [
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."RAW_membership_transaction_header_f55637" CLONE "CDC"."PEICUST"."F55637"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."RAW_sales_executive_membership_commission_transactions_f57637" CLONE "CDC"."PEICUST"."F57637"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."RAW_sales_executive_list_f57637l" CLONE "CDC"."PEICUST"."F57637L"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."RAW_user_defined_codes_f0005" CLONE "CDC"."PEICOM"."F0005"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."DICT_club_and_country_names" CLONE "PSMT_LAKEHOUSE_DEV"."AS400"."DICT_club_and_country_names"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."DICT_exchange_rates" CLONE "DOMO"."WRITEBACK"."DICT_exchange_rate"',
        'CREATE OR REPLACE TABLE "PLAYGROUND"."{{ schema }}"."DICT_membership_tax_by_country" CLONE "PSMT_LAKEHOUSE_DEV"."MANUAL_FILES"."DICT_MEMBERSHIP_TAX_BY_COUNTRY"'
    ] %}

    {% for stmt in clone_statements %}
        {% set rendered_stmt = stmt | replace("{{ schema }}", schema) %}
        {{ log("Executing: " ~ rendered_stmt, info=True) }}
        {{ run_query(rendered_stmt) }}
    {% endfor %}
{% endmacro %}
