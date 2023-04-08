WITH usaf_cases_info_active_base AS (
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
        'active' AS case_status,
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
),
usaf_cases_info_active_final AS (
    SELECT
        *,
        CASE
            WHEN rank_abv = 'AB' THEN 'enlisted'
            WHEN rank_abv = 'Amn' THEN 'enlisted'
            WHEN rank_abv = 'A1C' THEN 'enlisted'
            WHEN rank_abv = 'SrA' THEN 'enlisted'
            WHEN rank_abv = 'Sgt' THEN 'enlisted'
            WHEN rank_abv = 'SSgt' THEN 'enlisted'
            WHEN rank_abv = 'TSgt' THEN 'enlisted'
            WHEN rank_abv = 'MSgt' THEN 'enlisted'
            WHEN rank_abv = 'SMSgt' THEN 'enlisted'
            WHEN rank_abv = 'CMSgt' THEN 'enlisted'
            WHEN rank_abv = '2d Lt' THEN 'officer'
            WHEN rank_abv = '1st Lt' THEN 'officer'
            WHEN rank_abv = '1st Lt (E)' THEN 'officer'
            WHEN rank_abv = 'Capt' THEN 'officer'
            WHEN rank_abv = 'Capt (E)' THEN 'officer'
            WHEN rank_abv = 'Maj' THEN 'officer'
            WHEN rank_abv = 'Lt Col' THEN 'officer'
            WHEN rank_abv = 'Col' THEN 'officer'
            WHEN rank_abv = 'Maj Gen' THEN 'officer'
            WHEN rank_abv = 'AFC1' THEN 'cadet'
            WHEN rank_abv = 'AFC2' THEN 'cadet'
            WHEN rank_abv = 'AFC3' THEN 'cadet'
            WHEN rank_abv = 'AFC4' THEN 'cadet'
            WHEN rank_abv = 'Spc3' THEN 'enlisted'
            ELSE 'unassigned'
        END AS rank_type
    FROM
        usaf_cases_info_active_base
)
SELECT
    *
FROM
    usaf_cases_info_active_final
