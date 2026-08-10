CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.analytics.process_overview`

AS

WITH case_statistics AS (

    SELECT

        COUNT(*) AS total_cases,

        COUNTIF(is_analysis_ready)
            AS analysis_ready_cases,

        AVG(
            IF(
                is_analysis_ready,
                case_duration_hours,
                NULL
            )
        ) AS average_case_duration_hours,

        APPROX_QUANTILES(
            IF(
                is_analysis_ready,
                case_duration_hours,
                NULL
            ),
            100
            IGNORE NULLS
        )[OFFSET(50)]
            AS median_case_duration_hours,

        APPROX_QUANTILES(
            IF(
                is_analysis_ready,
                case_duration_hours,
                NULL
            ),
            100
            IGNORE NULLS
        )[OFFSET(90)]
            AS p90_case_duration_hours,

        AVG(event_count)
            AS average_events_per_case,

        COUNTIF(has_rework)
            AS rework_cases,

        SAFE_DIVIDE(
            COUNTIF(has_rework),
            COUNT(*)
        ) AS rework_case_rate

    FROM
        `p2p-process-mining-pipeline.analytics.case_kpis`

),

event_statistics AS (

    SELECT

        COUNT(*) AS total_events,

        COUNT(DISTINCT activity)
            AS total_activities,

        SAFE_DIVIDE(
            COUNTIF(is_resource_missing),
            COUNT(*)
        ) AS missing_resource_rate,

        SAFE_DIVIDE(
            COUNTIF(is_timestamp_outlier),
            COUNT(*)
        ) AS timestamp_outlier_rate

    FROM
        `p2p-process-mining-pipeline.staging.events`

),

variant_statistics AS (

    SELECT

        COUNT(*) AS total_variants,

        MAX(case_share)
            AS top_variant_case_share

    FROM
        `p2p-process-mining-pipeline.analytics.process_variants`

)

SELECT

    CURRENT_TIMESTAMP()
        AS calculation_timestamp_utc,

    case_statistics.*,
    event_statistics.*,
    variant_statistics.*

FROM
    case_statistics

CROSS JOIN
    event_statistics

CROSS JOIN
    variant_statistics;