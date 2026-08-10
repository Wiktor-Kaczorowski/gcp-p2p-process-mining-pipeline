-- ============================================================
-- CASE KPI RECONCILIATION
-- ============================================================

SELECT

    COUNT(*) AS case_kpi_rows,

    COUNT(DISTINCT case_id)
        AS unique_cases

FROM
    `p2p-process-mining-pipeline.analytics.case_kpis`;


-- ============================================================
-- VARIANT RECONCILIATION
-- ============================================================

SELECT

    COUNT(*) AS process_variant_count,

    SUM(case_count)
        AS cases_represented_by_variants

FROM
    `p2p-process-mining-pipeline.analytics.process_variants`;


-- ============================================================
-- ACTIVITY RECONCILIATION
-- ============================================================

SELECT

    COUNT(*) AS activity_count

FROM
    `p2p-process-mining-pipeline.analytics.activity_performance`;


-- ============================================================
-- TRANSITION RECONCILIATION
-- ============================================================

SELECT

    COUNT(*) AS transition_types,

    SUM(transition_count)
        AS total_transitions

FROM
    `p2p-process-mining-pipeline.analytics.transitions`;


-- ============================================================
-- TOP PROCESS VARIANTS
-- ============================================================

SELECT

    variant_rank,

    case_count,

    ROUND(
        case_share * 100,
        2
    ) AS case_share_percent,

    ROUND(
        median_case_duration_hours,
        2
    ) AS median_duration_hours,

    ROUND(
        rework_case_rate * 100,
        2
    ) AS rework_rate_percent,

    process_variant

FROM
    `p2p-process-mining-pipeline.analytics.process_variants`

ORDER BY
    variant_rank

LIMIT 20;


-- ============================================================
-- FREQUENT SLOW TRANSITIONS
-- ============================================================

SELECT

    from_activity,
    to_activity,

    transition_count,
    case_count,

    ROUND(
        median_transition_seconds / 3600,
        2
    ) AS median_transition_hours,

    ROUND(
        p90_transition_seconds / 3600,
        2
    ) AS p90_transition_hours

FROM
    `p2p-process-mining-pipeline.analytics.transitions`

WHERE
    transition_count >= 100

ORDER BY
    p90_transition_seconds DESC

LIMIT 20;