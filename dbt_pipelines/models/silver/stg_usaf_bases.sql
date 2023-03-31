
{{ config(materialized='view') }}


with usaf_bases as (
    
    SELECT * FROM {{ source('bronze', 'usaf_bases_raw') }} LIMIT 10
)

SELECT * FROM usaf_bases




