WITH usaf_cases_info_inactive AS (
    SELECT
        primary_key,
        "caseInfo.rankAbbrv" AS rank_abv,
        "caseInfo.rank" AS rank_full,
        "caseInfo.rankOrder" AS rank_order,
        "caseInfo.baseCode" AS base_code,
        "caseInfo.nameFirst" AS first_name,
        "caseInfo.nameLast" AS last_name,
        "caseInfo.caseType" AS case_type,
        "caseInfo.tr140aUrl" AS trial_url,
        "caseInfo.trialCompletionDate" AS trial_completion_date,
        "caseInfo.verdict" AS verdict,
        "caseInfo.forum" AS forum,
        "caseInfo.sentence" AS sentence,
        "caseInfo.fk_id" AS foreign_key,
        updated_at
    FROM
        {{ ref("usaf_cases_info_raw_snapshot") }}
    WHERE
        dbt_valid_to IS NULL
        AND "caseInfo.scheduledTrialStartDate" IS NULL
)
SELECT
    *
FROM
    usaf_cases_info_inactive
