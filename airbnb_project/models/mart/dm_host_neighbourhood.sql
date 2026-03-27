{{ config(materialized='view', schema='gold') }}

SELECT 
    host_id,
    lga_name,
    month_year,
    COUNT(DISTINCT listing_id) as num_listings,
    SUM(estimated_revenue) as total_estimated_revenue,
    -- Estimated Revenue per Host
    SUM(estimated_revenue) / COUNT(DISTINCT host_id) as revenue_per_host
FROM {{ ref('fact_airbnb_analytics') }}
GROUP BY 1, 2, 3