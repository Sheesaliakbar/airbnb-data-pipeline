{{ config(materialized='table', schema='gold') }}

SELECT
    listing_id,
    host_id,
    host_name,
    room_type,
    property_type,
    suburb_name,
    accommodates,
    price_per_night
FROM {{ ref('stg_listings') }}