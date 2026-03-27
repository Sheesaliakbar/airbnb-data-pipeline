{{ config(materialized='view', schema='gold') }}

SELECT 
    property_type,
    room_type,
    accommodates,
    month_year,
    -- Metrics
    COUNT(listing_id) as total_listings,
    SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) as active_listings,
    AVG(price_per_night) as avg_price,
    SUM(estimated_revenue) as total_revenue,
    AVG(availability_30) as avg_availability_30
FROM {{ ref('fact_airbnb_analytics') }} f
JOIN {{ ref('dim_listings') }} d ON f.listing_id = d.listing_id
GROUP BY 1, 2, 3, 4