{{ config(materialized='table', schema='gold') }}

WITH lga_names AS (
    SELECT * FROM {{ ref('stg_lga_codes') }}
),
census_metrics AS (
    SELECT * FROM {{ ref('stg_census') }}
)

SELECT
    n.lga_code,
    n.lga_name,
    c.total_population,
    c.median_household_income,
    c.pop_age_25_34,
    c.pop_age_35_44,
    c.avg_household_size
FROM lga_names n
LEFT JOIN census_metrics c ON n.lga_code::text = REPLACE(c.lga_code::text, 'LGA', '')