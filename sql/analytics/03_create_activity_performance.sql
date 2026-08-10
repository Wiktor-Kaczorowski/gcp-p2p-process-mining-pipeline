CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.analytics.activity_performance`

CLUSTER BY activity

AS

SELECT

    activity,

    COUNT(*) AS event_count,

    COUNT(DISTINCT case_id)
        AS case_count,

    COUNT(DISTINCT resource_id)
        AS distinct_resource_count,

    COUNTIF(is_case_start)
        AS case_start_count,

    COUNTIF(is_case_end)
        AS case_end_count,

    COUNTIF(is_resource_missing)
        AS missing_resource_event_count,

    SAFE_DIVIDE(
        COUNTIF(is_resource_missing),
        COUNT(*)
    ) AS missing_resource_rate,

    COUNTIF(is_timestamp_outlier)
        AS timestamp_outlier_event_count,

    AVG(
        seconds_since_previous_event
    ) AS average_seconds_since_previous_event,

    APPROX_QUANTILES(
        seconds_since_previous_event,
        100
        IGNORE NULLS
    )[OFFSET(50)]
        AS median_seconds_since_previous_event,

    APPROX_QUANTILES(
        seconds_since_previous_event,
        100
        IGNORE NULLS
    )[OFFSET(90)]
        AS p90_seconds_since_previous_event,

    AVG(
        seconds_to_next_event
    ) AS average_seconds_to_next_event,

    APPROX_QUANTILES(
        seconds_to_next_event,
        100
        IGNORE NULLS
    )[OFFSET(50)]
        AS median_seconds_to_next_event,

    APPROX_QUANTILES(
        seconds_to_next_event,
        100
        IGNORE NULLS
    )[OFFSET(90)]
        AS p90_seconds_to_next_event

FROM
    `p2p-process-mining-pipeline.staging.events`

GROUP BY
    activity;