{{ config(materialized='view', schema='silver') }}

WITH all_listings_union AS (
    -- Har select mein mahine ka context dene ke liye hum scraped_date use kar rahe hain
    SELECT *, '2020-05-01'::date as month_year FROM {{ source('raw_data', 'listings_05_2020') }} UNION ALL
    SELECT *, '2020-06-01'::date as month_year FROM {{ source('raw_data', 'listings_06_2020') }} UNION ALL
    SELECT *, '2020-07-01'::date as month_year FROM {{ source('raw_data', 'listings_07_2020') }} UNION ALL
    SELECT *, '2020-08-01'::date as month_year FROM {{ source('raw_data', 'listings_08_2020') }} UNION ALL
    SELECT *, '2020-09-01'::date as month_year FROM {{ source('raw_data', 'listings_09_2020') }} UNION ALL
    SELECT *, '2020-10-01'::date as month_year FROM {{ source('raw_data', 'listings_10_2020') }} UNION ALL
    SELECT *, '2020-11-01'::date as month_year FROM {{ source('raw_data', 'listings_11_2020') }} UNION ALL
    SELECT *, '2020-12-01'::date as month_year FROM {{ source('raw_data', 'listings_12_2020') }} UNION ALL
    SELECT *, '2021-01-01'::date as month_year FROM {{ source('raw_data', 'listings_01_2021') }} UNION ALL
    SELECT *, '2021-02-01'::date as month_year FROM {{ source('raw_data', 'listings_02_2021') }} UNION ALL
    SELECT *, '2021-03-01'::date as month_year FROM {{ source('raw_data', 'listings_03_2021') }} UNION ALL
    SELECT *, '2021-04-01'::date as month_year FROM {{ source('raw_data', 'listings_04_2021') }}
)

SELECT
    "LISTING_ID"::bigint AS listing_id,
    month_year, -- Tracking ke liye zaruri hai
    "SCRAPED_DATE"::date AS scraped_date,
    "HOST_ID"::bigint AS host_id,
    "HOST_NAME"::varchar AS host_name,
    "HOST_IS_SUPERHOST"::varchar AS host_is_superhost,
    LOWER(TRIM("LISTING_NEIGHBOURHOOD"))::varchar AS suburb_name, -- Cleaned Suburb
    "PROPERTY_TYPE"::varchar AS property_type,
    "ROOM_TYPE"::varchar AS room_type,
    "ACCOMMODATES"::int AS accommodates,
    -- Professional Cleaning of Price: Remove $ and ,
    NULLIF(REPLACE(REPLACE("PRICE", '$', ''), ',', ''), '')::numeric AS price_per_night,
    "HAS_AVAILABILITY"::varchar AS has_availability,
    "AVAILABILITY_30"::int AS availability_30,
    "NUMBER_OF_REVIEWS"::int AS number_of_reviews,
    "REVIEW_SCORES_RATING"::numeric AS review_scores_rating
FROM all_listings_union