WITH usaf_charges AS (
    SELECT
        stg_usaf_cases_charges.primary_key,
        stg_usaf_cases_charges.case_id,
        stg_usaf_cases_charges.prefix,
        stg_usaf_charges.code,
        stg_usaf_charges.article,
        stg_usaf_charges.article_suffix,
        stg_usaf_charges.detail,
        stg_usaf_cases_charges.updated_at
    FROM
        {{ ref('stg_usaf_cases_charges') }} AS stg_usaf_cases_charges
        LEFT JOIN {{ ref('stg_usaf_charges') }} AS stg_usaf_charges
        ON stg_usaf_cases_charges.code = stg_usaf_charges.code
)
SELECT
    *
FROM
    usaf_charges
