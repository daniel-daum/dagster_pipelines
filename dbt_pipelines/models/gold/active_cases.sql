SELECT
    charges.primary_key,
    charges.case_id,
    charges.prefix,
    charges.code,
    charges.article,
    charges.article_suffix,
    charges.detail,
    active.case_status,
    active.rank_type,
    active.rank_abv,
    active.first_name,
    active.last_name,
    active.case_type,
    active.base_name,
    active.state,
    active.state_abv,
    active.estimated_trial_days,
    active.schd_trial_start_date,
    active.schd_trial_end_date,
    charges.updated_at
FROM
    {{ ref('usaf_charges') }} AS charges
    RIGHT JOIN {{ ref('usaf_cases_active') }} AS active
    ON charges.case_id = active.foreign_key
