{% snapshot usaf_bases_raw_snapshot %}


{{
    config(
      target_schema='dwh_bronze',
      unique_key='primary_key',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('docket','usaf_bases_raw') }}

{% endsnapshot %}