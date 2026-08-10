CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.analytics.case_kpis`

CLUSTER BY
    company_id,
    vendor_id,
    start_activity,
    end_activity

AS

WITH event_statistics AS (

    SELECT

        case_id,

        COUNT(*) AS event_count,

        COUNT(DISTINCT activity)
            AS distinct_activity_count,

        COUNT(*) - COUNT(DISTINCT activity)
            AS repeated_activity_event_count,

        COUNTIF(is_resource_missing)
            AS missing_resource_event_count,

        COUNTIF(is_timestamp_outlier)
            AS timestamp_outlier_event_count,

        COUNT(DISTINCT resource_id)
            AS distinct_resource_count,

        COUNT(DISTINCT ingestion_batch_id)
            AS ingestion_batch_count

    FROM
        `p2p-process-mining-pipeline.staging.events`

    GROUP BY
        case_id

)

SELECT

    cases.case_id,
    cases.case_name,

    cases.case_start_timestamp,
    cases.case_end_timestamp,

    cases.case_duration_seconds,
    cases.case_duration_hours,
    cases.case_duration_days,

    events.event_count,
    events.distinct_activity_count,

    events.repeated_activity_event_count,

    events.repeated_activity_event_count > 0
        AS has_rework,

    SAFE_DIVIDE(
        events.repeated_activity_event_count,
        events.event_count
    ) AS rework_event_rate,

    events.distinct_resource_count,

    events.missing_resource_event_count,

    SAFE_DIVIDE(
        events.missing_resource_event_count,
        events.event_count
    ) AS missing_resource_rate,

    events.timestamp_outlier_event_count,

    events.timestamp_outlier_event_count = 0
        AS is_analysis_ready,

    events.ingestion_batch_count,

    cases.start_activity,
    cases.end_activity,

    cases.company_id,
    cases.vendor_id,

    cases.purchasing_document_id,
    cases.document_type,
    cases.purchasing_document_category,

    cases.item_type,
    cases.item_category,

    cases.spend_area,
    cases.sub_spend_area,
    cases.spend_classification,

    cases.maximum_cumulative_net_worth_eur,

    cases.first_ingestion_date,
    cases.last_ingestion_date

FROM
    `p2p-process-mining-pipeline.staging.cases`
        AS cases

INNER JOIN
    event_statistics
        AS events

USING (case_id);