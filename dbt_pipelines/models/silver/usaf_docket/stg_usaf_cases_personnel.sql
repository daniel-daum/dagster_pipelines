WITH stg_usaf_cases_personnel_base AS (
    SELECT
        primary_key,
        case_id,
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
),
stg_usaf_cases_personnel_final AS (
    SELECT
        primary_key,
        case_id,
        title,
        SPLIT_PART(
            names,
            ',',
            1
        ) AS personnel_one,
        SPLIT_PART(
            names,
            ',',
            2
        ) AS personnel_two,
        SPLIT_PART(
            names,
            ',',
            3
        ) AS personnel_three,
        SPLIT_PART(
            names,
            ',',
            4
        ) AS personnel_four,
        updated_at
    FROM
        stg_usaf_cases_personnel_base
)
SELECT
    *
FROM
    stg_usaf_cases_personnel_final
