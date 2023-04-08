WITH usaf_cases_inactive AS (
    SELECT
        inactive_cases.primary_key,
        inactive_cases.foreign_key,
        inactive_cases.case_status,
        inactive_cases.rank_type,
        inactive_cases.rank_abv,
        inactive_cases.first_name,
        inactive_cases.last_name,
        inactive_cases.case_type,
        usaf_bases.base_name,
        usaf_bases.state,
        usaf_bases.state_abv,
        inactive_cases.trial_completion_date,
        inactive_cases.verdict,
        inactive_cases.sentence,
        inactive_cases.forum,
        inactive_cases.trial_url,
        inactive_cases.acquitted,
        inactive_cases.confinement,
        inactive_cases.discharge,
        inactive_cases.forefeiture,
        inactive_cases.labor,
        inactive_cases.reduction,
        inactive_cases.reprimand,
        inactive_cases.no_trial_set,
        inactive_cases.restriction,
        inactive_cases.updated_at
    FROM
        {{ ref("stg_usaf_cases_inactive") }} AS inactive_cases
        LEFT JOIN {{ ref("stg_usaf_bases") }} AS usaf_bases
        ON inactive_cases.base_code = usaf_bases.base
)
SELECT
    *
FROM
    usaf_cases_inactive
