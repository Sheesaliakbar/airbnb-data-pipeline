{% snapshot census_snapshot %}
{{
    config(
      target_schema='silver',
      unique_key='lga_code',
      strategy='check',
      check_cols=['total_population', 'median_household_income']
    )
}}
select * from {{ ref('stg_census') }}
{% endsnapshot %}