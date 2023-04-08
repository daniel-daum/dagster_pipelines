WITH usaf_cases_info_inactive_base AS (
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
        'inactive' AS case_status,
        updated_at
    FROM
        {{ ref("usaf_cases_info_raw_snapshot") }}
    WHERE
        dbt_valid_to IS NULL
        AND "caseInfo.scheduledTrialStartDate" IS NULL
),
usaf_cases_info_inactive_intermediate AS (
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
        usaf_cases_info_inactive_base
),
usaf_cases_info_inactive_final AS (
    SELECT
        *,CASE
            WHEN sentence LIKE '%Acquitted%' THEN TRUE
            ELSE FALSE
        END AS acquitted,
        CASE
            WHEN sentence LIKE '%Confinement%'
            OR sentence LIKE '%confinement%' THEN TRUE
            ELSE FALSE
        END AS confinement,
        CASE
            WHEN sentence LIKE '%Discharge%'
            OR sentence LIKE '%BCD%'
            OR sentence LIKE '%DD%'
            OR sentence LIKE '%DIS%' THEN TRUE
            ELSE FALSE
        END AS discharge,
        CASE
            WHEN sentence LIKE '%Forfeiture%'
            OR sentence LIKE '%forfeitures%'
            OR sentence LIKE '%Fine%' THEN TRUE
            ELSE FALSE
        END AS forefeiture,
        CASE
            WHEN sentence LIKE '%labor%' THEN TRUE
            ELSE FALSE
        END AS labor,
        CASE
            WHEN sentence LIKE '%Red%'
            OR sentence LIKE '%Reduction%' THEN TRUE
            ELSE FALSE
        END AS reduction,
        CASE
            WHEN sentence LIKE '%Reprimand%' THEN TRUE
            ELSE FALSE
        END AS reprimand,
        CASE
            WHEN sentence LIKE '%No Trial Set%' THEN TRUE
            ELSE FALSE
        END AS no_trial_set,
        CASE
            WHEN sentence LIKE '%restriction%' THEN TRUE
            ELSE FALSE
        END AS restriction
    FROM
        usaf_cases_info_inactive_intermediate
)
SELECT
    *
FROM
    usaf_cases_info_inactive_final
