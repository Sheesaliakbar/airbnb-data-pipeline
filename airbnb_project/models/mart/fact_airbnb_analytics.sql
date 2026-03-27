{{ config(materialized='table', schema='gold') }}

WITH listings AS (
    SELECT * FROM {{ ref('dim_listings') }}
),
suburb_map AS (
    SELECT * FROM {{ ref('stg_lga_suburb') }}
),
lga_info AS (
    SELECT * FROM {{ ref('dim_lga_profile') }}
)

SELECT
    l.listing_id,
    l.price_per_night,
    l.suburb_name,
    s.lga_name,
    i.lga_code,
    i.total_population,
    i.median_household_income,
    i.pop_age_25_34
FROM listings l

INNER JOIN suburb_map s ON LOWER(TRIM(l.suburb_name)) = LOWER(TRIM(s.suburb_name))
INNER JOIN lga_info i ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(i.lga_name))