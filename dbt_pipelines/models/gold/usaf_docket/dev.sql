SELECT
    sentence,
    CASE
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
    dwh_gold.usaf_cases_inactive
