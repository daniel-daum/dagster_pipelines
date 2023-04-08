WITH usaf_cases_active AS(
    SELECT
        active_cases.primary_key,
        active_cases.foreign_key,
        active_cases.case_status,
        active_cases.rank_type,
        active_cases.rank_abv,
        active_cases.first_name,
        active_cases.last_name,
        active_cases.case_type,
        usaf_bases.base_name,
        usaf_bases.state,
        usaf_bases.state_abv,
        CAST(active_cases.estimated_trial_days AS integer),
        active_cases.schd_trial_start_date,
        active_cases.schd_trial_end_date,
        active_cases.updated_at
    FROM
        {{ ref("stg_usaf_cases_active") }} AS active_cases
        LEFT JOIN {{ ref("stg_usaf_bases") }} AS usaf_bases
        ON active_cases.base_code = usaf_bases.base
)
SELECT
    *
FROM
    usaf_cases_active
