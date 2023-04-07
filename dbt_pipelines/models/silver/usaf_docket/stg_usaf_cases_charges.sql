WITH stg_usaf_cases_charges AS (
    SELECT
        primary_key,
        case_id,
        code,
        prefix,
        updated_at
    FROM
        {{ ref('usaf_cases_charges_raw_snapshot') }}
    WHERE
        dbt_valid_to IS NULL
)
SELECT
    *
FROM
    stg_usaf_cases_charges
