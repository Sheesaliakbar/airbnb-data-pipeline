{{ config(materialized='table', schema='silver') }}

SELECT
    "LGA_CODE"::int AS lga_code,
    "LGA_NAME" AS lga_name
FROM {{ source('raw_data', 'nsw_lga_codes') }}