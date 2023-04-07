WITH stg_usaf_charges AS (
    SELECT
        primary_key,
        code,
        article,
        definition AS detail,
        updated_at
    FROM
        {{ ref("usaf_charges_raw_snapshot") }}
    WHERE
        dbt_valid_to IS NULL
)
SELECT
    *
FROM
    stg_usaf_charges
