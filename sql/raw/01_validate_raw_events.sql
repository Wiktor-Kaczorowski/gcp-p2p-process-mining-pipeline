-- RAW event-table reconciliation

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS unique_event_ids,
    COUNT(DISTINCT case_id) AS unique_cases,
    COUNT(DISTINCT activity) AS unique_activities,
    COUNT(DISTINCT ingestion_batch_id) AS ingestion_batches
FROM `p2p-process-mining-pipeline.raw.events`;


-- Batch-level reconciliation

SELECT
    ingestion_batch_id,
    COUNT(*) AS row_count
FROM `p2p-process-mining-pipeline.raw.events`
GROUP BY ingestion_batch_id
ORDER BY ingestion_batch_id;


-- Load audit history

SELECT
    batch_id,
    expected_rows,
    loaded_rows,
    status,
    job_id,
    load_timestamp_utc
FROM `p2p-process-mining-pipeline.raw.batch_load_history`
ORDER BY load_timestamp_utc DESC;