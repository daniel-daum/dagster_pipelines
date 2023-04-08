WITH stg_usaf_charges AS (
    SELECT
        primary_key,
        code,
        regexp_replace(article, '[^0-9]+', '', 'g') as article,
        regexp_replace(article, '[0-9]+', '', 'g') as article_suffix,
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
