# Incremental Batch Strategy

## Objective

The original BPI Challenge 2019 dataset is a historical event-log
snapshot.

To simulate an operational data pipeline, the standardized event
dataset is divided into deterministic ingestion batches.

## Batch size

Each batch contains a maximum of:

100,000 events.

The complete dataset contains 1,595,923 events and is therefore
divided into 16 batches.

## Ordering

Before batching, events are ordered using:

1. event_timestamp
2. source_row_number

This creates a deterministic chronological sequence.

## Business time vs ingestion time

The following fields represent different concepts:

- `event_timestamp` — original business-event timestamp
- `simulated_ingestion_timestamp_utc` — technical pipeline arrival time

The simulated ingestion timestamp does not replace or modify the
original business timestamp.

## Cases across batches

A process case can occur in multiple batches.

This intentionally simulates a real incremental pipeline where new
events can arrive for an already known Purchase-to-Pay case.

## Batch metadata

Every batch contains:

- ingestion_batch_id
- ingestion_batch_sequence
- simulated_ingestion_timestamp_utc
- simulated_ingestion_date
- batch_row_number

## Batch manifest

A manifest is generated for every pipeline batch.

It contains:

- batch identifier
- row count
- case count
- activity count
- timestamp range
- file size
- SHA-256 checksum
- processing status

The manifest will later be used for ingestion monitoring,
idempotency checks and troubleshooting.