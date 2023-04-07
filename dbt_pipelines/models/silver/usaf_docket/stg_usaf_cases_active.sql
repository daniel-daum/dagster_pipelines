WITH usaf_cases_info_active AS (
    SELECT
        primary_key,
        "caseInfo.rankAbbrv" AS rank_abv,
        "caseInfo.rank" AS rank_full,
        "caseInfo.rankOrder" AS rank_order,
        "caseInfo.baseCode" AS base_code,
        "caseInfo.nameFirst" AS first_name,
        "caseInfo.nameLast" AS last_name,
        "caseInfo.caseType" AS case_type,
        "caseInfo.scheduledTrialStartDate" AS schd_trial_start_date,
        "caseInfo.scheduledTrialEndDate" AS schd_trial_end_date,
        "caseInfo.estimatedTrialDays" AS estimated_trial_days,
        "caseInfo.fk_id" AS foreign_key,
        updated_at
    FROM
        {{ ref(
            "usaf_cases_info_raw_snapshot"
        ) }}
    WHERE
        dbt_valid_to IS NULL
        AND "caseInfo.tr140aUrl" IS NULL
        AND "caseInfo.trialCompletionDate" IS NULL
        AND "caseInfo.verdict" IS NULL
        AND "caseInfo.forum" IS NULL
        AND "caseInfo.sentence" IS NULL
        AND "caseInfo.rankAbbrv" IS NOT NULL
)
SELECT
    *
FROM
    usaf_cases_info_active
