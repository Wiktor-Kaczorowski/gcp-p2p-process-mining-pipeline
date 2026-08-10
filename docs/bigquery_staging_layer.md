# BigQuery STAGING Layer

## Objective

The STAGING layer transforms the immutable RAW event data into a
clean, process-oriented event log ready for process mining and
analytical modelling.

## Tables

### staging.events

Contains one row per process event.

The transformation:

- removes duplicate event IDs defensively
- normalizes text fields
- orders events within each process case
- reconstructs cases spanning multiple ingestion batches
- calculates previous and next process activities
- calculates inter-event durations
- identifies case-start and case-end events
- preserves timestamp-quality classifications
- identifies missing technical resources

The table is partitioned by:

`event_date`

and clustered by:

- `case_id`
- `activity`
- `vendor_id`
- `company_id`

### staging.cases

Contains one row per process case.

It provides:

- case start timestamp
- case end timestamp
- case duration
- event count
- distinct activity count
- start activity
- end activity
- company and vendor attributes
- timestamp-quality metrics
- number of ingestion batches contributing to the case

## Layer separation

RAW is organized around technical ingestion.

STAGING is organized around process-event semantics.

For example:

- RAW partition field: `simulated_ingestion_date`
- STAGING partition field: `event_date`

This separation preserves source traceability while providing an
analysis-ready representation of the process.

## Reconciliation

The STAGING build validates that:

- RAW and STAGING event counts match
- event IDs remain unique
- case counts reconcile
- mandatory event-log fields remain populated