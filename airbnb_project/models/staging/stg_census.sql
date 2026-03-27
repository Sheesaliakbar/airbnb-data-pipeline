{{ config(materialized='table', schema='silver') }}

WITH g01_refined AS (
    SELECT
        REPLACE("LGA_CODE_2016", 'LGA', '')::int AS lga_code,
        "TOT_P_P" AS total_population,
        "AGE_25_34_YR_P" AS pop_age_25_34,
        "AGE_35_44_YR_P" AS pop_age_35_44,
        "HIGH_YR_SCHL_COMP_YR_12_EQ_P" AS high_school_graduates
    FROM {{ source('raw_data', 'nsw_lga_census_2016_g01') }}
),

g02_refined AS (
    SELECT
        REPLACE("LGA_CODE_2016", 'LGA', '')::int AS lga_code ,
        "MEDIAN_AGE_PERSONS" AS median_age,
        "MEDIAN_TOT_HHD_INC_WEEKLY" AS median_household_income,
        "MEDIAN_RENT_WEEKLY" AS median_rent_weekly,
        "AVERAGE_HOUSEHOLD_SIZE" AS avg_household_size
    FROM {{ source('raw_data', 'nsw_lga_census_2016_g02') }}
)

SELECT 
    g1.*,
    g2.median_age,
    g2.median_household_income,
    g2.median_rent_weekly,
    g2.avg_household_size
FROM g01_refined g1
JOIN g02_refined g2 ON g1.lga_code = g2.lga_code