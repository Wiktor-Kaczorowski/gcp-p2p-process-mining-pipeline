# Google Cloud Purchase-to-Pay Process Mining Data Pipeline

## Project objective

The objective of this project is to build an end-to-end data pipeline
for monitoring and analysing a Purchase-to-Pay process.

The project will use:

- BPI Challenge 2019 event data
- Python and PM4Py
- Google Cloud Storage
- BigQuery
- data-quality checks
- pipeline monitoring
- Looker Studio

## Planned data flow

BPI Challenge 2019 → Python → GCS → BigQuery → Looker Studio

## Project status

- [x] Source-data profiling
- [x] Event-log standardization
- [x] Incremental batch generation
- [x] Controlled data-quality scenarios
- [x] Automated batch validation
- [x] Google Cloud Storage ingestion
- [x] BigQuery raw layer
- [x] BigQuery staging layer
- [x] Cloud data-quality monitoring
- [ ] Process analytics model
- [ ] Looker Studio dashboard

## Data source

The project uses the anonymized BPI Challenge 2019 event log.

The event log represents a real-life Purchase-to-Pay process and contains
activities related to purchase orders, goods receipts, invoices and payments.

The original XES file is not stored in this repository. Profiling reports
describing the source schema are available in `artifacts/profiling`.

## Incremental ingestion simulation

The canonical event dataset is divided into 16 deterministic
ingestion batches.

Each batch contains up to 100,000 events and includes technical
ingestion metadata.

A batch manifest stores row counts, timestamp ranges, checksums
and processing status.

This structure will be used to simulate incremental ingestion into
Google Cloud Storage and BigQuery.

## Automated validation

Every ingestion batch is validated before cloud ingestion.

The validation framework checks:

- file readability
- schema consistency
- mandatory process-mining fields
- event uniqueness
- row counts
- batch identifiers
- idempotency

The validation suite is tested against 16 clean batches and
6 controlled failure scenarios.

## Cloud ingestion

Validated Parquet batches are uploaded to a Google Cloud Storage
landing zone using the Google Cloud Python client.

The ingestion process includes:

- pre-upload data-quality verification
- manifest-based file-size validation
- SHA-256 integrity verification
- cloud object metadata
- idempotent upload handling
- upload monitoring reports