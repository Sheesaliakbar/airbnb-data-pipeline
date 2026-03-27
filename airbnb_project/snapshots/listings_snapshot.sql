{% snapshot listings_snapshot %}
{{
    config(
      target_schema='silver',
      unique_key='listing_id',
      strategy='check',
      check_cols=['price_per_night', 'review_rating', 'room_type']
    )
}}
select * from {{ ref('stg_listings') }}
{% endsnapshot %}