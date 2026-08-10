-- ============================================
-- STAGING EVENTS RECONCILIATION
-- ============================================

SELECT
    COUNT(*) AS staging_rows,
    COUNT(DISTINCT event_id)
        AS unique_event_ids,
    COUNT(DISTINCT case_id)
        AS unique_cases,
    COUNT(DISTINCT activity)
        AS unique_activities
FROM
    `p2p-process-mining-pipeline.staging.events`;


-- ============================================
-- RAW VS STAGING
-- ============================================

SELECT

    (
        SELECT COUNT(*)
        FROM
            `p2p-process-mining-pipeline.raw.events`
    ) AS raw_rows,

    (
        SELECT COUNT(*)
        FROM
            `p2p-process-mining-pipeline.staging.events`
    ) AS staging_rows;


-- ============================================
-- MANDATORY FIELDS
-- ============================================

SELECT
    COUNTIF(case_id IS NULL)
        AS missing_case_id,
    COUNTIF(activity IS NULL)
        AS missing_activity,
    COUNTIF(event_timestamp IS NULL)
        AS missing_event_timestamp
FROM
    `p2p-process-mining-pipeline.staging.events`;


-- ============================================
-- EVENT-ID DUPLICATES
-- ============================================

SELECT
    event_id,
    COUNT(*) AS row_count
FROM
    `p2p-process-mining-pipeline.staging.events`
GROUP BY
    event_id
HAVING
    COUNT(*) > 1;


-- ============================================
-- CASE RECONCILIATION
-- ============================================

SELECT
    COUNT(*) AS cases,
    COUNT(DISTINCT case_id)
        AS unique_case_ids
FROM
    `p2p-process-mining-pipeline.staging.cases`;


-- ============================================
-- PROCESS SEQUENCE TEST
-- ============================================

SELECT
    COUNT(*) AS invalid_sequences
FROM (
    SELECT
        case_id,
        COUNT(*) AS event_count,
        MAX(process_event_sequence)
            AS maximum_sequence
    FROM
        `p2p-process-mining-pipeline.staging.events`
    GROUP BY
        case_id
)
WHERE
    event_count != maximum_sequence;


-- ============================================
-- TIMESTAMP QUALITY
-- ============================================

SELECT
    timestamp_quality_status,
    COUNT(*) AS event_count
FROM
    `p2p-process-mining-pipeline.staging.events`
GROUP BY
    timestamp_quality_status
ORDER BY
    event_count DESC;