WITH stg_usaf_cases_personnel AS (
    SELECT
        primary_key,
        case_id,
        "order" AS num_order,
        REPLACE(
            title,
            ':',
            ''
        ) AS title,
        NAME AS names,
        updated_at
    FROM
        {{ ref("usaf_cases_personnel_raw_snapshot") }}
    WHERE
        dbt_valid_to IS NULL
)
SELECT
    *
FROM
    stg_usaf_cases_personnel
