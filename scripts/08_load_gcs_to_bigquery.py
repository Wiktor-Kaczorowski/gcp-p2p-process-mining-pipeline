from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BATCH_MANIFEST_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "batches"
    / "batch_manifest.csv"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "cloud"
)

RAW_DATASET = "raw"
EVENTS_TABLE = "events"
LOAD_HISTORY_TABLE = "batch_load_history"

PARTITION_FIELD = "simulated_ingestion_date"

CLUSTERING_FIELDS = [
    "case_id",
    "activity",
    "ingestion_batch_id",
]


def load_configuration() -> tuple[str, str, str]:
    """Load project configuration from .env."""

    load_dotenv(
        PROJECT_ROOT / ".env"
    )

    project_id = os.getenv(
        "GCP_PROJECT_ID"
    )

    region = os.getenv(
        "GCP_REGION"
    )

    bucket_name = os.getenv(
        "GCS_BUCKET_NAME"
    )

    missing = []

    if not project_id:
        missing.append(
            "GCP_PROJECT_ID"
        )

    if not region:
        missing.append(
            "GCP_REGION"
        )

    if not bucket_name:
        missing.append(
            "GCS_BUCKET_NAME"
        )

    if missing:
        raise ValueError(
            "Missing environment variables: "
            + ", ".join(missing)
        )

    return (
        project_id,
        region,
        bucket_name,
    )


def load_manifest() -> pd.DataFrame:
    """Load the clean ingestion batch manifest."""

    if not BATCH_MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Manifest not found: "
            f"{BATCH_MANIFEST_PATH}"
        )

    manifest = pd.read_csv(
        BATCH_MANIFEST_PATH
    )

    required_columns = {
        "batch_id",
        "batch_sequence",
        "row_count",
        "file_size_bytes",
        "sha256",
    }

    missing_columns = (
        required_columns
        - set(manifest.columns)
    )

    if missing_columns:
        raise ValueError(
            "Manifest is missing columns: "
            + ", ".join(
                sorted(missing_columns)
            )
        )

    return manifest.sort_values(
        "batch_sequence"
    ).reset_index(
        drop=True
    )


def table_exists(
    client: bigquery.Client,
    table_id: str,
) -> bool:
    """Return True if a BigQuery table exists."""

    try:
        client.get_table(
            table_id
        )
        return True

    except NotFound:
        return False


def verify_dataset(
    client: bigquery.Client,
    project_id: str,
    region: str,
) -> None:
    """Verify the RAW BigQuery dataset."""

    dataset_id = (
        f"{project_id}."
        f"{RAW_DATASET}"
    )

    dataset = client.get_dataset(
        dataset_id
    )

    if (
        dataset.location or ""
    ).lower() != region.lower():
        raise RuntimeError(
            "BigQuery dataset region mismatch. "
            f"Expected {region}, "
            f"received {dataset.location}."
        )

    print(
        f"BigQuery dataset verified: "
        f"{dataset_id}"
    )

    print(
        f"Dataset location: "
        f"{dataset.location}"
    )


def verify_bucket(
    client: storage.Client,
    bucket_name: str,
    region: str,
) -> storage.Bucket:
    """Verify GCS bucket and region."""

    bucket = client.get_bucket(
        bucket_name
    )

    if (
        bucket.location or ""
    ).lower() != region.lower():
        raise RuntimeError(
            "Cloud Storage region mismatch. "
            f"Expected {region}, "
            f"received {bucket.location}."
        )

    print(
        f"GCS bucket verified: "
        f"gs://{bucket_name}"
    )

    return bucket


def verify_gcs_batch(
    bucket: storage.Bucket,
    batch_row: pd.Series,
) -> str:
    """
    Verify that a batch exists in GCS and
    matches manifest metadata.
    """

    batch_id = str(
        batch_row["batch_id"]
    )

    object_name = (
        f"raw/events/"
        f"{batch_id}/"
        f"events.parquet"
    )

    blob = bucket.blob(
        object_name
    )

    if not blob.exists():
        raise FileNotFoundError(
            f"GCS object not found: "
            f"gs://{bucket.name}/"
            f"{object_name}"
        )

    blob.reload()

    expected_size = int(
        batch_row[
            "file_size_bytes"
        ]
    )

    actual_size = int(
        blob.size or 0
    )

    if actual_size != expected_size:
        raise RuntimeError(
            f"{batch_id}: GCS file-size "
            f"mismatch. Expected "
            f"{expected_size}, "
            f"received {actual_size}."
        )

    metadata = (
        blob.metadata or {}
    )

    remote_sha256 = metadata.get(
        "sha256"
    )

    expected_sha256 = str(
        batch_row["sha256"]
    )

    if remote_sha256 != expected_sha256:
        raise RuntimeError(
            f"{batch_id}: GCS SHA-256 "
            "metadata does not match "
            "the batch manifest."
        )

    return (
        f"gs://{bucket.name}/"
        f"{object_name}"
    )


def create_load_history_table(
    client: bigquery.Client,
    project_id: str,
) -> str:
    """Create RAW batch load audit table."""

    table_id = (
        f"{project_id}."
        f"{RAW_DATASET}."
        f"{LOAD_HISTORY_TABLE}"
    )

    if table_exists(
        client,
        table_id,
    ):
        return table_id

    schema = [
        bigquery.SchemaField(
            "run_id",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "batch_id",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "gcs_uri",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "expected_rows",
            "INTEGER",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "loaded_rows",
            "INTEGER",
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            "status",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "job_id",
            "STRING",
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            "sha256",
            "STRING",
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            "message",
            "STRING",
            mode="NULLABLE",
        ),
        bigquery.SchemaField(
            "load_timestamp_utc",
            "TIMESTAMP",
            mode="REQUIRED",
        ),
    ]

    table = bigquery.Table(
        table_id,
        schema=schema,
    )

    table.time_partitioning = (
        bigquery.TimePartitioning(
            field="load_timestamp_utc"
        )
    )

    table.clustering_fields = [
        "batch_id",
        "status",
    ]

    client.create_table(
        table
    )

    print(
        f"Created table: {table_id}"
    )

    return table_id


def get_existing_batch_row_count(
    client: bigquery.Client,
    events_table_id: str,
    batch_id: str,
) -> int:
    """Count rows already loaded for one batch."""

    if not table_exists(
        client,
        events_table_id,
    ):
        return 0

    sql = f"""
        SELECT COUNT(*) AS row_count
        FROM `{events_table_id}`
        WHERE ingestion_batch_id = @batch_id
    """

    job_config = (
        bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "batch_id",
                    "STRING",
                    batch_id,
                )
            ]
        )
    )

    rows = client.query(
        sql,
        job_config=job_config,
    ).result()

    result = next(
        iter(rows)
    )

    return int(
        result["row_count"]
    )


def create_load_job_config() -> bigquery.LoadJobConfig:
    """Create configuration for Parquet batch load."""

    job_config = (
        bigquery.LoadJobConfig()
    )

    job_config.source_format = (
        bigquery.SourceFormat.PARQUET
    )

    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_APPEND
    )

    job_config.create_disposition = (
        bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    job_config.time_partitioning = (
        bigquery.TimePartitioning(
            field=PARTITION_FIELD
        )
    )

    job_config.clustering_fields = (
        CLUSTERING_FIELDS
    )

    job_config.labels = {
        "pipeline": "p2p_process_mining",
        "layer": "raw",
    }

    return job_config


def record_load_history(
    client: bigquery.Client,
    history_table_id: str,
    record: dict[str, Any],
) -> None:
    """Append one audit record."""

    errors = client.insert_rows_json(
        history_table_id,
        [record],
    )

    if errors:
        raise RuntimeError(
            "Failed to write batch load "
            f"history: {errors}"
        )


def load_single_batch(
    bq_client: bigquery.Client,
    bucket: storage.Bucket,
    project_id: str,
    batch_row: pd.Series,
    run_id: str,
    history_table_id: str,
) -> dict[str, Any]:
    """Load one GCS Parquet batch into BigQuery."""

    batch_id = str(
        batch_row["batch_id"]
    )

    expected_rows = int(
        batch_row["row_count"]
    )

    sha256 = str(
        batch_row["sha256"]
    )

    gcs_uri = verify_gcs_batch(
        bucket=bucket,
        batch_row=batch_row,
    )

    events_table_id = (
        f"{project_id}."
        f"{RAW_DATASET}."
        f"{EVENTS_TABLE}"
    )

    existing_rows = (
        get_existing_batch_row_count(
            client=bq_client,
            events_table_id=events_table_id,
            batch_id=batch_id,
        )
    )

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    if existing_rows == expected_rows:
        record = {
            "run_id": run_id,
            "batch_id": batch_id,
            "gcs_uri": gcs_uri,
            "expected_rows": (
                expected_rows
            ),
            "loaded_rows": (
                existing_rows
            ),
            "status": "SKIPPED",
            "job_id": None,
            "sha256": sha256,
            "message": (
                "Expected batch row count "
                "already exists in raw.events."
            ),
            "load_timestamp_utc": (
                timestamp
            ),
        }

        record_load_history(
            client=bq_client,
            history_table_id=(
                history_table_id
            ),
            record=record,
        )

        return record

    if existing_rows != 0:
        record = {
            "run_id": run_id,
            "batch_id": batch_id,
            "gcs_uri": gcs_uri,
            "expected_rows": (
                expected_rows
            ),
            "loaded_rows": (
                existing_rows
            ),
            "status": "CONFLICT",
            "job_id": None,
            "sha256": sha256,
            "message": (
                "Partial or unexpected batch "
                "already exists in raw.events."
            ),
            "load_timestamp_utc": (
                timestamp
            ),
        }

        record_load_history(
            client=bq_client,
            history_table_id=(
                history_table_id
            ),
            record=record,
        )

        return record

    job_config = (
        create_load_job_config()
    )

    load_job = (
        bq_client.load_table_from_uri(
            gcs_uri,
            events_table_id,
            job_config=job_config,
            location=os.getenv(
                "GCP_REGION"
            ),
        )
    )

    load_job.result()

    loaded_rows = (
        get_existing_batch_row_count(
            client=bq_client,
            events_table_id=events_table_id,
            batch_id=batch_id,
        )
    )

    if loaded_rows == expected_rows:
        status = "LOADED"

        message = (
            "Batch loaded successfully "
            "and row count reconciled."
        )

    else:
        status = "FAILED"

        message = (
            f"Expected {expected_rows} rows "
            f"after load, found "
            f"{loaded_rows}."
        )

    record = {
        "run_id": run_id,
        "batch_id": batch_id,
        "gcs_uri": gcs_uri,
        "expected_rows": (
            expected_rows
        ),
        "loaded_rows": (
            loaded_rows
        ),
        "status": status,
        "job_id": (
            load_job.job_id
        ),
        "sha256": sha256,
        "message": message,
        "load_timestamp_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }

    record_load_history(
        client=bq_client,
        history_table_id=(
            history_table_id
        ),
        record=record,
    )

    return record


def validate_final_raw_table(
    client: bigquery.Client,
    events_table_id: str,
) -> dict[str, int]:
    """Run final BigQuery reconciliation metrics."""

    sql = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT event_id)
                AS unique_event_ids,
            COUNT(DISTINCT case_id)
                AS unique_cases,
            COUNT(DISTINCT activity)
                AS unique_activities,
            COUNT(DISTINCT ingestion_batch_id)
                AS ingestion_batches
        FROM `{events_table_id}`
    """

    result = next(
        iter(
            client.query(
                sql
            ).result()
        )
    )

    return {
        "total_rows": int(
            result[
                "total_rows"
            ]
        ),
        "unique_event_ids": int(
            result[
                "unique_event_ids"
            ]
        ),
        "unique_cases": int(
            result[
                "unique_cases"
            ]
        ),
        "unique_activities": int(
            result[
                "unique_activities"
            ]
        ),
        "ingestion_batches": int(
            result[
                "ingestion_batches"
            ]
        ),
    }


def save_schema_report(
    client: bigquery.Client,
    events_table_id: str,
) -> None:
    """Save BigQuery RAW table schema locally."""

    table = client.get_table(
        events_table_id
    )

    rows = []

    for field in table.schema:
        rows.append(
            {
                "column_name": field.name,
                "data_type": field.field_type,
                "mode": field.mode,
            }
        )

    schema_dataframe = (
        pd.DataFrame(
            rows
        )
    )

    schema_dataframe.to_csv(
        OUTPUT_DIRECTORY
        / "bigquery_raw_schema.csv",
        index=False,
    )


def save_reports(
    results: pd.DataFrame,
    metrics: dict[str, int],
    project_id: str,
    run_id: str,
) -> None:
    """Save BigQuery load reports."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = (
        OUTPUT_DIRECTORY
        / "bigquery_raw_load_report.csv"
    )

    summary_path = (
        OUTPUT_DIRECTORY
        / "bigquery_raw_summary.json"
    )

    results.to_csv(
        report_path,
        index=False,
        encoding="utf-8",
    )

    successful_statuses = {
        "LOADED",
        "SKIPPED",
    }

    success_count = int(
        results[
            "status"
        ]
        .isin(
            successful_statuses
        )
        .sum()
    )

    summary = {
        "project_id": project_id,
        "dataset": RAW_DATASET,
        "table": EVENTS_TABLE,
        "run_id": run_id,
        "batches_processed": int(
            len(results)
        ),
        "successful_batches": (
            success_count
        ),
        "status_counts": (
            results[
                "status"
            ]
            .value_counts()
            .to_dict()
        ),
        **metrics,
        "raw_load_success": (
            success_count
            == len(results)
        ),
    }

    with summary_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print("\nCreated BigQuery reports:")
    print(f"- {report_path}")
    print(f"- {summary_path}")


def main() -> None:
    print(
        "Starting GCS to BigQuery RAW ingestion."
    )

    (
        project_id,
        region,
        bucket_name,
    ) = load_configuration()

    manifest = load_manifest()

    bq_client = bigquery.Client(
        project=project_id,
        location=region,
    )

    storage_client = (
        storage.Client(
            project=project_id
        )
    )

    verify_dataset(
        client=bq_client,
        project_id=project_id,
        region=region,
    )

    bucket = verify_bucket(
        client=storage_client,
        bucket_name=bucket_name,
        region=region,
    )

    history_table_id = (
        create_load_history_table(
            client=bq_client,
            project_id=project_id,
        )
    )

    run_id = str(
        uuid.uuid4()
    )

    results = []

    print("\nLoading batches")
    print("---------------")

    for _, batch_row in (
        manifest.iterrows()
    ):
        result = load_single_batch(
            bq_client=bq_client,
            bucket=bucket,
            project_id=project_id,
            batch_row=batch_row,
            run_id=run_id,
            history_table_id=(
                history_table_id
            ),
        )

        results.append(
            result
        )

        print(
            f"{result['batch_id']}: "
            f"{result['status']} | "
            f"{result['loaded_rows']:,} rows"
        )

        if result[
            "status"
        ] not in {
            "LOADED",
            "SKIPPED",
        }:
            raise RuntimeError(
                f"{result['batch_id']} "
                f"finished with "
                f"{result['status']}."
            )

    results_dataframe = (
        pd.DataFrame(
            results
        )
    )

    events_table_id = (
        f"{project_id}."
        f"{RAW_DATASET}."
        f"{EVENTS_TABLE}"
    )

    metrics = (
        validate_final_raw_table(
            client=bq_client,
            events_table_id=(
                events_table_id
            ),
        )
    )

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    save_schema_report(
        client=bq_client,
        events_table_id=events_table_id,
    )

    save_reports(
        results=results_dataframe,
        metrics=metrics,
        project_id=project_id,
        run_id=run_id,
    )

    print("\nBigQuery RAW summary")
    print("--------------------")

    print(
        f"Rows: "
        f"{metrics['total_rows']:,}"
    )

    print(
        f"Unique event IDs: "
        f"{metrics['unique_event_ids']:,}"
    )

    print(
        f"Cases: "
        f"{metrics['unique_cases']:,}"
    )

    print(
        f"Activities: "
        f"{metrics['unique_activities']:,}"
    )

    print(
        f"Batches: "
        f"{metrics['ingestion_batches']}"
    )

    expected_total_rows = int(
        manifest[
            "row_count"
        ].sum()
    )

    success = (
        metrics[
            "total_rows"
        ]
        == expected_total_rows
        and metrics[
            "unique_event_ids"
        ]
        == expected_total_rows
        and metrics[
            "ingestion_batches"
        ]
        == len(manifest)
    )

    if success:
        print(
            "\nBIGQUERY RAW LOAD "
            "COMPLETED SUCCESSFULLY."
        )

    else:
        raise RuntimeError(
            "Final RAW reconciliation failed."
        )


if __name__ == "__main__":
    main()