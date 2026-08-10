# BigQuery RAW Layer

## Objective

Validated Parquet batches stored in Google Cloud Storage are
incrementally loaded into the BigQuery RAW layer.

## Source

Google Cloud Storage:

`raw/events/<batch_id>/events.parquet`

## Destination

BigQuery:

`raw.events`

## Table design

The RAW events table is partitioned by:

`simulated_ingestion_date`

and clustered by:

- `case_id`
- `activity`
- `ingestion_batch_id`

## Incremental loading

Each ingestion batch is loaded independently.

Before a batch is loaded, the pipeline verifies:

- the GCS object exists
- object size matches the manifest
- SHA-256 metadata matches the manifest
- the batch is not already fully present in BigQuery

## Idempotency

If the expected number of records for an ingestion batch already
exists in the RAW table, the batch is skipped.

If a non-zero but unexpected number of records exists, the pipeline
returns a conflict instead of appending more records.

## Audit history

Each load attempt is recorded in:

`raw.batch_load_history`

The audit table stores:

- pipeline run ID
- batch ID
- GCS URI
- expected rows
- loaded rows
- load status
- BigQuery job ID
- SHA-256
- timestamp
- diagnostic message

## Reconciliation

After all batch loads complete, the pipeline validates:

- total event count
- unique event identifiers
- case count
- activity count
- number of ingestion batches