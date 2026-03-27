{% snapshot lga_suburb_snapshot %}
{{
    config(
      target_schema='silver',
      unique_key='suburb_name',
      strategy='check',
      check_cols=['lga_name']
    )
}}
select * from {{ ref('stg_lga_suburb') }}
{% endsnapshot %}