# Data Quality Validation Framework

## Objective

Every ingestion batch is validated before it is accepted by the
analytical pipeline.

The validation layer is designed to detect file, schema, record
and batch-level problems.

## Validation rules

### FILE_READABLE

Checks whether the Parquet file can be successfully opened.

Possible failure:

- corrupted file
- invalid Parquet metadata
- incomplete file transfer

### DQ_SCHEMA_COLUMNS

Compares the received schema with the expected canonical schema.

Possible failure:

- required column removed
- source column renamed
- unexpected column introduced

### DQ_NOT_NULL_CASE_ID

Ensures every process event has a case identifier.

### DQ_NOT_NULL_ACTIVITY

Ensures every process event contains an activity.

### DQ_NOT_NULL_EVENT_TIMESTAMP

Ensures every process event has a process timestamp.

### DQ_UNIQUE_EVENT_ID

Checks whether event identifiers are unique inside the batch.

### DQ_ROW_COUNT

Compares the received batch row count with the batch manifest.

### DQ_BATCH_ID

Ensures that all records belong to the expected ingestion batch.

### DQ_IDEMPOTENCY

Checks whether the ingestion batch has already been processed.

A previously processed batch must not be loaded again.

## Validation result model

Every validation rule produces:

- dataset_name
- batch_id
- rule_id
- status
- failed_rows
- message

Statuses include:

- PASS
- FAIL
- SKIPPED

## Controlled failure testing

The validation framework is tested against both:

- 16 valid ingestion batches
- 6 intentionally invalid scenarios

The invalid scenarios include:

- duplicate events
- missing case identifiers
- missing timestamps
- schema drift
- duplicate batch delivery
- corrupted Parquet file

The validation suite is successful only when all clean batches pass
and every controlled failure is detected.