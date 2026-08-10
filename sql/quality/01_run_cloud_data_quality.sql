-- ============================================================
-- CLOUD DATA QUALITY MONITORING
-- ============================================================

CREATE TABLE IF NOT EXISTS
    `p2p-process-mining-pipeline.monitoring.data_quality_results`
(
    run_id STRING,
    run_timestamp_utc TIMESTAMP,
    rule_id STRING,
    rule_name STRING,
    severity STRING,
    status STRING,
    failed_rows INT64,
    total_rows INT64,
    failure_rate FLOAT64,
    message STRING
)

PARTITION BY DATE(run_timestamp_utc)

CLUSTER BY
    rule_id,
    status,
    severity;


CREATE TABLE IF NOT EXISTS
    `p2p-process-mining-pipeline.monitoring.pipeline_run_summary`
(
    run_id STRING,
    run_timestamp_utc TIMESTAMP,
    rules_executed INT64,
    passed_rules INT64,
    warning_rules INT64,
    failed_rules INT64,
    overall_status STRING
)

PARTITION BY DATE(run_timestamp_utc)

CLUSTER BY
    overall_status;


-- ============================================================
-- EXECUTE DATA QUALITY RULES
-- ============================================================

INSERT INTO
    `p2p-process-mining-pipeline.monitoring.data_quality_results`

WITH

raw_statistics AS (

    SELECT

        COUNT(*) AS total_rows,

        COUNT(
            DISTINCT event_id
        ) AS unique_event_ids

    FROM
        `p2p-process-mining-pipeline.raw.events`

),

staging_statistics AS (

    SELECT

        COUNT(*) AS total_rows,

        COUNT(
            DISTINCT case_id
        ) AS unique_cases,

        COUNTIF(
            case_id IS NULL
        ) AS missing_case_ids,

        COUNTIF(
            activity IS NULL
        ) AS missing_activities,

        COUNTIF(
            event_timestamp IS NULL
        ) AS missing_timestamps,

        COUNTIF(
            seconds_since_previous_event < 0
            OR seconds_to_next_event < 0
        ) AS negative_process_intervals,

        COUNTIF(
            is_timestamp_outlier
        ) AS timestamp_outlier_events,

        COUNTIF(
            is_resource_missing
        ) AS missing_resource_events

    FROM
        `p2p-process-mining-pipeline.staging.events`

),

case_statistics AS (

    SELECT

        COUNT(*) AS case_rows,

        COUNT(
            DISTINCT case_id
        ) AS unique_case_ids

    FROM
        `p2p-process-mining-pipeline.staging.cases`

),

sequence_statistics AS (

    SELECT

        COUNT(*) AS total_cases,

        COUNTIF(
            minimum_sequence != 1
            OR maximum_sequence != event_count
            OR distinct_sequences != event_count
        ) AS invalid_sequence_cases

    FROM (

        SELECT

            case_id,

            COUNT(*) AS event_count,

            COUNT(
                DISTINCT process_event_sequence
            ) AS distinct_sequences,

            MIN(
                process_event_sequence
            ) AS minimum_sequence,

            MAX(
                process_event_sequence
            ) AS maximum_sequence

        FROM
            `p2p-process-mining-pipeline.staging.events`

        GROUP BY
            case_id

    )

),

latest_batch_history AS (

    SELECT
        batch_id,
        expected_rows

    FROM
        `p2p-process-mining-pipeline.raw.batch_load_history`

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY batch_id
            ORDER BY load_timestamp_utc DESC
        ) = 1

),

raw_batch_counts AS (

    SELECT

        ingestion_batch_id AS batch_id,

        COUNT(*) AS actual_rows

    FROM
        `p2p-process-mining-pipeline.raw.events`

    GROUP BY
        ingestion_batch_id

),

batch_statistics AS (

    SELECT

        COUNT(*) AS total_batches,

        COUNTIF(
            COALESCE(
                raw_batch_counts.actual_rows,
                0
            )
            != latest_batch_history.expected_rows
        ) AS failed_batches

    FROM
        latest_batch_history

    LEFT JOIN
        raw_batch_counts

    USING (
        batch_id
    )

),

all_rules AS (

    -- --------------------------------------------------------
    -- UNIQUE EVENT ID
    -- --------------------------------------------------------

    SELECT

        @run_id AS run_id,

        @run_timestamp_utc
            AS run_timestamp_utc,

        'DQ_RAW_EVENT_ID_UNIQUE'
            AS rule_id,

        'RAW event identifiers are unique'
            AS rule_name,

        'CRITICAL'
            AS severity,

        IF(
            total_rows = unique_event_ids,
            'PASS',
            'FAIL'
        ) AS status,

        total_rows
            - unique_event_ids
            AS failed_rows,

        total_rows,

        SAFE_DIVIDE(
            total_rows - unique_event_ids,
            total_rows
        ) AS failure_rate,

        FORMAT(
            'RAW contains %d rows and %d unique event IDs.',
            total_rows,
            unique_event_ids
        ) AS message

    FROM
        raw_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- RAW / STAGING RECONCILIATION
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_RAW_STAGING_ROW_RECONCILIATION',

        'RAW and STAGING row counts reconcile',

        'CRITICAL',

        IF(
            raw_statistics.total_rows
            = staging_statistics.total_rows,
            'PASS',
            'FAIL'
        ),

        ABS(
            raw_statistics.total_rows
            - staging_statistics.total_rows
        ),

        raw_statistics.total_rows,

        SAFE_DIVIDE(
            ABS(
                raw_statistics.total_rows
                - staging_statistics.total_rows
            ),
            raw_statistics.total_rows
        ),

        FORMAT(
            'RAW rows: %d; STAGING rows: %d.',
            raw_statistics.total_rows,
            staging_statistics.total_rows
        )

    FROM
        raw_statistics

    CROSS JOIN
        staging_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- CASE RECONCILIATION
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_CASE_RECONCILIATION',

        'STAGING event cases reconcile with case table',

        'CRITICAL',

        IF(
            staging_statistics.unique_cases
            = case_statistics.case_rows,
            'PASS',
            'FAIL'
        ),

        ABS(
            staging_statistics.unique_cases
            - case_statistics.case_rows
        ),

        staging_statistics.unique_cases,

        SAFE_DIVIDE(
            ABS(
                staging_statistics.unique_cases
                - case_statistics.case_rows
            ),
            staging_statistics.unique_cases
        ),

        FORMAT(
            'Event-log cases: %d; case-table rows: %d.',
            staging_statistics.unique_cases,
            case_statistics.case_rows
        )

    FROM
        staging_statistics

    CROSS JOIN
        case_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- MANDATORY PROCESS FIELDS
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_MANDATORY_EVENT_FIELDS',

        'Mandatory process-mining fields are populated',

        'CRITICAL',

        IF(
            missing_case_ids
            + missing_activities
            + missing_timestamps
            = 0,
            'PASS',
            'FAIL'
        ),

        missing_case_ids
            + missing_activities
            + missing_timestamps,

        total_rows,

        SAFE_DIVIDE(
            missing_case_ids
            + missing_activities
            + missing_timestamps,
            total_rows
        ),

        FORMAT(
            'Missing case_id: %d; activity: %d; timestamp: %d.',
            missing_case_ids,
            missing_activities,
            missing_timestamps
        )

    FROM
        staging_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- PROCESS SEQUENCE
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_PROCESS_SEQUENCE_INTEGRITY',

        'Event sequence is complete within each case',

        'CRITICAL',

        IF(
            invalid_sequence_cases = 0,
            'PASS',
            'FAIL'
        ),

        invalid_sequence_cases,

        total_cases,

        SAFE_DIVIDE(
            invalid_sequence_cases,
            total_cases
        ),

        FORMAT(
            '%d cases contain invalid process-event sequences.',
            invalid_sequence_cases
        )

    FROM
        sequence_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- BATCH RECONCILIATION
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_BATCH_LOAD_RECONCILIATION',

        'Loaded batch row counts match expected row counts',

        'CRITICAL',

        IF(
            failed_batches = 0,
            'PASS',
            'FAIL'
        ),

        failed_batches,

        total_batches,

        SAFE_DIVIDE(
            failed_batches,
            total_batches
        ),

        FORMAT(
            '%d of %d ingestion batches fail row-count reconciliation.',
            failed_batches,
            total_batches
        )

    FROM
        batch_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- NEGATIVE PROCESS INTERVALS
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'DQ_NEGATIVE_PROCESS_DURATION',

        'Inter-event durations are non-negative',

        'CRITICAL',

        IF(
            negative_process_intervals = 0,
            'PASS',
            'FAIL'
        ),

        negative_process_intervals,

        total_rows,

        SAFE_DIVIDE(
            negative_process_intervals,
            total_rows
        ),

        FORMAT(
            '%d events contain negative inter-event durations.',
            negative_process_intervals
        )

    FROM
        staging_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- TIMESTAMP QUALITY MONITORING
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'MON_TIMESTAMP_QUALITY',

        'Known timestamp-quality anomalies are monitored',

        'WARNING',

        IF(
            timestamp_outlier_events = 0,
            'PASS',
            'WARN'
        ),

        timestamp_outlier_events,

        total_rows,

        SAFE_DIVIDE(
            timestamp_outlier_events,
            total_rows
        ),

        FORMAT(
            '%d events are outside the expected timestamp period.',
            timestamp_outlier_events
        )

    FROM
        staging_statistics


    UNION ALL


    -- --------------------------------------------------------
    -- RESOURCE COMPLETENESS MONITORING
    -- --------------------------------------------------------

    SELECT

        @run_id,

        @run_timestamp_utc,

        'MON_MISSING_RESOURCE',

        'Missing event resources are monitored',

        'WARNING',

        IF(
            missing_resource_events = 0,
            'PASS',
            'WARN'
        ),

        missing_resource_events,

        total_rows,

        SAFE_DIVIDE(
            missing_resource_events,
            total_rows
        ),

        FORMAT(
            '%d events have no resource identifier.',
            missing_resource_events
        )

    FROM
        staging_statistics

)

SELECT
    *
FROM
    all_rules;


-- ============================================================
-- PIPELINE RUN SUMMARY
-- ============================================================

INSERT INTO
    `p2p-process-mining-pipeline.monitoring.pipeline_run_summary`

SELECT

    @run_id,

    @run_timestamp_utc,

    COUNT(*) AS rules_executed,

    COUNTIF(
        status = 'PASS'
    ) AS passed_rules,

    COUNTIF(
        status = 'WARN'
    ) AS warning_rules,

    COUNTIF(
        status = 'FAIL'
    ) AS failed_rules,

    CASE

        WHEN COUNTIF(
            status = 'FAIL'
        ) > 0
            THEN 'FAIL'

        WHEN COUNTIF(
            status = 'WARN'
        ) > 0
            THEN 'WARN'

        ELSE 'PASS'

    END AS overall_status

FROM
    `p2p-process-mining-pipeline.monitoring.data_quality_results`

WHERE
    run_id = @run_id;