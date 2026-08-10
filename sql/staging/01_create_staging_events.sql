CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.staging.events`

PARTITION BY event_date

CLUSTER BY
    case_id,
    activity,
    vendor_id,
    company_id

AS

WITH source_events AS (

    SELECT
        *
    FROM
        `p2p-process-mining-pipeline.raw.events`

),

deduplicated_events AS (

    SELECT
        *
    FROM source_events

    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY
                simulated_ingestion_timestamp_utc DESC,
                source_row_number DESC
        ) = 1

),

normalized_events AS (

    SELECT

        event_id,

        NULLIF(
            TRIM(CAST(case_id AS STRING)),
            ''
        ) AS case_id,

        NULLIF(
            TRIM(CAST(case_name AS STRING)),
            ''
        ) AS case_name,

        NULLIF(
            TRIM(CAST(activity AS STRING)),
            ''
        ) AS activity,

        event_timestamp,

        DATE(event_timestamp) AS event_date,

        EXTRACT(
            YEAR
            FROM event_timestamp
        ) AS event_year,

        EXTRACT(
            MONTH
            FROM event_timestamp
        ) AS event_month,

        NULLIF(
            TRIM(CAST(resource_id AS STRING)),
            ''
        ) AS resource_id,

        NULLIF(
            TRIM(CAST(company_id AS STRING)),
            ''
        ) AS company_id,

        NULLIF(
            TRIM(CAST(vendor_id AS STRING)),
            ''
        ) AS vendor_id,

        NULLIF(
            TRIM(
                CAST(
                    purchasing_document_id
                    AS STRING
                )
            ),
            ''
        ) AS purchasing_document_id,

        NULLIF(
            TRIM(CAST(item_id AS STRING)),
            ''
        ) AS item_id,

        NULLIF(
            TRIM(
                CAST(
                    document_type
                    AS STRING
                )
            ),
            ''
        ) AS document_type,

        NULLIF(
            TRIM(
                CAST(
                    purchasing_document_category
                    AS STRING
                )
            ),
            ''
        ) AS purchasing_document_category,

        NULLIF(
            TRIM(CAST(item_type AS STRING)),
            ''
        ) AS item_type,

        NULLIF(
            TRIM(
                CAST(
                    item_category
                    AS STRING
                )
            ),
            ''
        ) AS item_category,

        NULLIF(
            TRIM(CAST(spend_area AS STRING)),
            ''
        ) AS spend_area,

        NULLIF(
            TRIM(
                CAST(
                    sub_spend_area
                    AS STRING
                )
            ),
            ''
        ) AS sub_spend_area,

        NULLIF(
            TRIM(
                CAST(
                    spend_classification
                    AS STRING
                )
            ),
            ''
        ) AS spend_classification,

        NULLIF(
            TRIM(
                CAST(
                    source_system
                    AS STRING
                )
            ),
            ''
        ) AS source_system,

        cumulative_net_worth_eur,

        gr_based_invoice_verification,

        goods_receipt_required,

        timestamp_quality_status,

        source_file,

        source_row_number,

        pipeline_batch_id,

        ingestion_batch_id,

        ingestion_batch_sequence,

        simulated_ingestion_timestamp_utc,

        simulated_ingestion_date,

        batch_row_number,

        processing_timestamp_utc

    FROM deduplicated_events

),

sequenced_events AS (

    SELECT

        *,

        ROW_NUMBER() OVER (
            PARTITION BY case_id
            ORDER BY
                event_timestamp,
                source_row_number
        ) AS process_event_sequence,

        COUNT(*) OVER (
            PARTITION BY case_id
        ) AS process_event_count,

        LAG(activity) OVER (
            PARTITION BY case_id
            ORDER BY
                event_timestamp,
                source_row_number
        ) AS previous_activity,

        LEAD(activity) OVER (
            PARTITION BY case_id
            ORDER BY
                event_timestamp,
                source_row_number
        ) AS next_activity,

        LAG(event_timestamp) OVER (
            PARTITION BY case_id
            ORDER BY
                event_timestamp,
                source_row_number
        ) AS previous_event_timestamp,

        LEAD(event_timestamp) OVER (
            PARTITION BY case_id
            ORDER BY
                event_timestamp,
                source_row_number
        ) AS next_event_timestamp

    FROM normalized_events

)

SELECT

    *,

    TIMESTAMP_DIFF(
        event_timestamp,
        previous_event_timestamp,
        SECOND
    ) AS seconds_since_previous_event,

    TIMESTAMP_DIFF(
        next_event_timestamp,
        event_timestamp,
        SECOND
    ) AS seconds_to_next_event,

    process_event_sequence = 1
        AS is_case_start,

    process_event_sequence = process_event_count
        AS is_case_end,

    timestamp_quality_status
        != 'expected_period'
        AS is_timestamp_outlier,

    resource_id IS NULL
        AS is_resource_missing

FROM sequenced_events;