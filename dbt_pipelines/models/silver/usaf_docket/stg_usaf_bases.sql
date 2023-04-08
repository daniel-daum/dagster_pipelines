WITH stg_usaf_bases AS (
    SELECT
        primary_key,
        base,
        base_name,
        state,
        state_abv,
        updated_at
    FROM
        {{ ref("usaf_bases_raw_snapshot") }}
    WHERE
        dbt_valid_to IS NULL
        AND primary_key != 'c561c9ff3fd6a4b0da8774d4bf24ba59' -- remove duplicated base
)
SELECT
    *
FROM
    stg_usaf_bases
