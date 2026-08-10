# Cloud Data Quality Monitoring

## Objective

The cloud monitoring layer validates the integrity of the
BigQuery RAW and STAGING datasets after pipeline execution.

Unlike local pre-ingestion validation, cloud monitoring verifies
the state of data after it has been loaded and transformed.

## Monitoring tables

### monitoring.data_quality_results

Stores one record for every data-quality rule and monitoring run.

Fields include:

- run identifier
- execution timestamp
- rule identifier
- severity
- status
- failed record count
- total record count
- failure rate
- diagnostic message

### monitoring.pipeline_run_summary

Stores one summary record per monitoring run.

Possible overall statuses:

- PASS
- WARN
- FAIL

## Critical rules

Critical rules validate:

- event identifier uniqueness
- RAW/STAGING row reconciliation
- case reconciliation
- mandatory process fields
- process-event sequence integrity
- ingestion batch reconciliation
- non-negative process intervals

A critical rule failure causes the monitoring script to fail.

## Warning rules

Warnings identify data characteristics that require attention but
do not invalidate the pipeline.

Current warning rules monitor:

- timestamp-quality outliers
- missing resource identifiers

Known source-data anomalies are therefore preserved and monitored
rather than silently removed.

## Monitoring history

Monitoring results are appended rather than overwritten.

This provides an audit trail that can later be visualized in a
dashboard and used for operational troubleshooting.