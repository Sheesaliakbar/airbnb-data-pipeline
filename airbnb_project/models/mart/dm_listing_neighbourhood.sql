{{ config(materialized='view', schema='gold') }}

WITH monthly_metrics AS (
    SELECT 
        lga_name,
        month_year,
        -- Total Listings
        COUNT(listing_id) as total_listings,
        -- Active Listings (Jahan has_availability = 't')
        SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) as active_listings,
        -- Prices for Active Listings
        MIN(CASE WHEN has_availability = 't' THEN price_per_night END) as min_price,
        MAX(CASE WHEN has_availability = 't' THEN price_per_night END) as max_price,
        AVG(CASE WHEN has_availability = 't' THEN price_per_night END) as avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price_per_night END) as median_price,
        -- Revenue Calculation
        SUM(estimated_revenue) as total_estimated_revenue
    FROM {{ ref('fact_airbnb_analytics') }}
    GROUP BY 1, 2
)

SELECT 
    *,
    -- Active Listing Rate (%)
    (active_listings::float / NULLIF(total_listings, 0)) * 100 as active_listing_rate,
    -- Revenue per Active Listing
    total_estimated_revenue / NULLIF(active_listings, 0) as rev_per_active_listing,
    -- Percentage Change (Previous Month) - Window Function
    LAG(active_listings) OVER (PARTITION BY lga_name ORDER BY month_year) as prev_month_active_listings
FROM monthly_metrics