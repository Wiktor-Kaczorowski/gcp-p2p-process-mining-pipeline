CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.staging.cases`

CLUSTER BY
    company_id,
    vendor_id

AS

SELECT

    case_id,

    ANY_VALUE(case_name) AS case_name,

    MIN(event_timestamp)
        AS case_start_timestamp,

    MAX(event_timestamp)
        AS case_end_timestamp,

    TIMESTAMP_DIFF(
        MAX(event_timestamp),
        MIN(event_timestamp),
        SECOND
    ) AS case_duration_seconds,

    TIMESTAMP_DIFF(
        MAX(event_timestamp),
        MIN(event_timestamp),
        HOUR
    ) AS case_duration_hours,

    TIMESTAMP_DIFF(
        MAX(event_timestamp),
        MIN(event_timestamp),
        DAY
    ) AS case_duration_days,

    COUNT(*) AS event_count,

    COUNT(
        DISTINCT activity
    ) AS distinct_activity_count,

    ARRAY_AGG(
        activity
        ORDER BY
            event_timestamp,
            source_row_number
        LIMIT 1
    )[OFFSET(0)]
        AS start_activity,

    ARRAY_AGG(
        activity
        ORDER BY
            event_timestamp DESC,
            source_row_number DESC
        LIMIT 1
    )[OFFSET(0)]
        AS end_activity,

    ANY_VALUE(company_id)
        AS company_id,

    ANY_VALUE(vendor_id)
        AS vendor_id,

    ANY_VALUE(
        purchasing_document_id
    ) AS purchasing_document_id,

    ANY_VALUE(document_type)
        AS document_type,

    ANY_VALUE(
        purchasing_document_category
    ) AS purchasing_document_category,

    ANY_VALUE(item_type)
        AS item_type,

    ANY_VALUE(item_category)
        AS item_category,

    ANY_VALUE(spend_area)
        AS spend_area,

    ANY_VALUE(sub_spend_area)
        AS sub_spend_area,

    ANY_VALUE(
        spend_classification
    ) AS spend_classification,

    MAX(
        cumulative_net_worth_eur
    ) AS maximum_cumulative_net_worth_eur,

    COUNTIF(
        is_timestamp_outlier
    ) AS timestamp_outlier_event_count,

    COUNTIF(
        is_resource_missing
    ) AS missing_resource_event_count,

    COUNT(
        DISTINCT ingestion_batch_id
    ) AS ingestion_batch_count,

    MIN(
        simulated_ingestion_date
    ) AS first_ingestion_date,

    MAX(
        simulated_ingestion_date
    ) AS last_ingestion_date

FROM
    `p2p-process-mining-pipeline.staging.events`

GROUP BY
    case_id;