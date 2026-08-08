# Google Cloud Storage Ingestion

## Objective

Validated local ingestion batches are uploaded to Google Cloud
Storage before being loaded into BigQuery.

Only batches that successfully pass the local data-quality
validation suite are eligible for cloud ingestion.

## Storage structure

```text
gs://<bucket>/

raw/
└── events/
    ├── batch_0001/events.parquet
    ├── batch_0002/events.parquet
    └── ...

manifests/
├── batch_manifest.csv
└── validation_batch_summary.csv