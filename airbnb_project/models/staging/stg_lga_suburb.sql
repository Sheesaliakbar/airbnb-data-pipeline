{{ config(materialized='table', schema='silver') }}

SELECT
    LOWER(TRIM("LGA_NAME")) AS lga_name,
    LOWER(TRIM("SUBURB_NAME")) AS suburb_name
FROM {{ source('raw_data', 'nsw_lga_suburb') }}