from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEAN_BATCH_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "batches"
    / "clean"
)

BATCH_MANIFEST_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "batches"
    / "batch_manifest.csv"
)

VALIDATION_SUMMARY_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "data_quality"
    / "validation_summary.json"
)

VALIDATION_BATCH_SUMMARY_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "data_quality"
    / "validation_batch_summary.csv"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "cloud"
)


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA-256 checksum of a local file."""

    sha256_hash = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256_hash.update(
                chunk
            )

    return sha256_hash.hexdigest()


def load_configuration() -> tuple[str, str, str]:
    """Load Google Cloud configuration from .env."""

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


def verify_local_validation() -> pd.DataFrame:
    """
    Verify that the local validation suite passed
    before any data is uploaded.
    """

    if not VALIDATION_SUMMARY_PATH.exists():
        raise FileNotFoundError(
            "Validation summary does not exist. "
            "Run scripts/06_validate_batches.py first."
        )

    with VALIDATION_SUMMARY_PATH.open(
        "r",
        encoding="utf-8",
    ) as file:
        validation_summary = json.load(
            file
        )

    clean_success = validation_summary.get(
        "clean_validation_success"
    )

    failure_detection_success = (
        validation_summary.get(
            "failure_detection_success"
        )
    )

    if not clean_success:
        raise RuntimeError(
            "Clean batch validation did not pass. "
            "Cloud upload aborted."
        )

    if not failure_detection_success:
        raise RuntimeError(
            "Controlled failure detection suite "
            "did not pass. Cloud upload aborted."
        )

    validation_batches = pd.read_csv(
        VALIDATION_BATCH_SUMMARY_PATH
    )

    failed_batches = validation_batches[
        validation_batches[
            "overall_status"
        ]
        != "PASS"
    ]

    if not failed_batches.empty:
        raise RuntimeError(
            "At least one clean batch has failed "
            "validation. Cloud upload aborted."
        )

    return validation_batches


def load_batch_manifest() -> pd.DataFrame:
    """Load local batch manifest."""

    if not BATCH_MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Batch manifest not found: "
            f"{BATCH_MANIFEST_PATH}"
        )

    manifest = pd.read_csv(
        BATCH_MANIFEST_PATH
    )

    required_columns = {
        "batch_id",
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
            "Batch manifest is missing columns: "
            + ", ".join(
                sorted(missing_columns)
            )
        )

    return manifest


def verify_bucket(
    client: storage.Client,
    bucket_name: str,
    expected_region: str,
) -> storage.Bucket:
    """Verify that the configured bucket exists."""

    bucket = client.get_bucket(
        bucket_name
    )

    actual_location = (
        bucket.location or ""
    ).lower()

    if actual_location != expected_region.lower():
        raise RuntimeError(
            f"Bucket location mismatch. "
            f"Expected {expected_region}, "
            f"received {bucket.location}."
        )

    print(
        f"Bucket verified: gs://{bucket_name}"
    )

    print(
        f"Bucket location: {bucket.location}"
    )

    return bucket


def verify_local_batch(
    batch_row: pd.Series,
) -> tuple[Path, str]:
    """Verify local file against manifest metadata."""

    batch_id = str(
        batch_row["batch_id"]
    )

    file_path = (
        CLEAN_BATCH_DIRECTORY
        / batch_id
        / "events.parquet"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Local batch file missing: "
            f"{file_path}"
        )

    actual_size = int(
        file_path.stat().st_size
    )

    expected_size = int(
        batch_row[
            "file_size_bytes"
        ]
    )

    if actual_size != expected_size:
        raise RuntimeError(
            f"{batch_id}: local file size "
            f"does not match manifest. "
            f"Expected {expected_size}, "
            f"received {actual_size}."
        )

    actual_sha256 = calculate_sha256(
        file_path
    )

    expected_sha256 = str(
        batch_row["sha256"]
    )

    if actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"{batch_id}: SHA-256 mismatch. "
            "Local batch may have changed."
        )

    return (
        file_path,
        actual_sha256,
    )


def upload_single_batch(
    bucket: storage.Bucket,
    batch_row: pd.Series,
) -> dict[str, Any]:
    """
    Upload one validated batch.

    Existing identical objects are skipped.
    Existing objects with different SHA-256 metadata
    are treated as conflicts.
    """

    batch_id = str(
        batch_row["batch_id"]
    )

    file_path, local_sha256 = (
        verify_local_batch(
            batch_row
        )
    )

    object_name = (
        f"raw/events/"
        f"{batch_id}/"
        f"events.parquet"
    )

    blob = bucket.blob(
        object_name
    )

    if blob.exists():
        blob.reload()

        remote_metadata = (
            blob.metadata or {}
        )

        remote_sha256 = (
            remote_metadata.get(
                "sha256"
            )
        )

        if remote_sha256 == local_sha256:
            return {
                "batch_id": batch_id,
                "local_file": str(
                    file_path.relative_to(
                        PROJECT_ROOT
                    )
                ),
                "gcs_uri": (
                    f"gs://{bucket.name}/"
                    f"{object_name}"
                ),
                "status": "SKIPPED",
                "row_count": int(
                    batch_row["row_count"]
                ),
                "file_size_bytes": int(
                    file_path.stat().st_size
                ),
                "sha256": local_sha256,
                "gcs_generation": (
                    blob.generation
                ),
                "message": (
                    "Identical object already "
                    "exists in Cloud Storage."
                ),
            }

        return {
            "batch_id": batch_id,
            "local_file": str(
                file_path.relative_to(
                    PROJECT_ROOT
                )
            ),
            "gcs_uri": (
                f"gs://{bucket.name}/"
                f"{object_name}"
            ),
            "status": "CONFLICT",
            "row_count": int(
                batch_row["row_count"]
            ),
            "file_size_bytes": int(
                file_path.stat().st_size
            ),
            "sha256": local_sha256,
            "gcs_generation": (
                blob.generation
            ),
            "message": (
                "Object already exists but "
                "SHA-256 metadata differs."
            ),
        }

    blob.metadata = {
        "batch_id": batch_id,
        "sha256": local_sha256,
        "pipeline": (
            "p2p-process-mining"
        ),
    }

    try:
        blob.upload_from_filename(
            str(file_path),
            content_type=(
                "application/octet-stream"
            ),
            checksum="auto",
            if_generation_match=0,
        )

    except PreconditionFailed:
        return {
            "batch_id": batch_id,
            "local_file": str(
                file_path.relative_to(
                    PROJECT_ROOT
                )
            ),
            "gcs_uri": (
                f"gs://{bucket.name}/"
                f"{object_name}"
            ),
            "status": "CONFLICT",
            "row_count": int(
                batch_row["row_count"]
            ),
            "file_size_bytes": int(
                file_path.stat().st_size
            ),
            "sha256": local_sha256,
            "gcs_generation": None,
            "message": (
                "Object was created by another "
                "process before upload completed."
            ),
        }

    blob.reload()

    remote_size = int(
        blob.size or 0
    )

    local_size = int(
        file_path.stat().st_size
    )

    if remote_size != local_size:
        return {
            "batch_id": batch_id,
            "local_file": str(
                file_path.relative_to(
                    PROJECT_ROOT
                )
            ),
            "gcs_uri": (
                f"gs://{bucket.name}/"
                f"{object_name}"
            ),
            "status": "FAILED",
            "row_count": int(
                batch_row["row_count"]
            ),
            "file_size_bytes": local_size,
            "sha256": local_sha256,
            "gcs_generation": (
                blob.generation
            ),
            "message": (
                "Remote object size does not "
                "match local file size."
            ),
        }

    return {
        "batch_id": batch_id,
        "local_file": str(
            file_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "gcs_uri": (
            f"gs://{bucket.name}/"
            f"{object_name}"
        ),
        "status": "UPLOADED",
        "row_count": int(
            batch_row["row_count"]
        ),
        "file_size_bytes": local_size,
        "sha256": local_sha256,
        "gcs_generation": (
            blob.generation
        ),
        "message": (
            "Upload completed and remote "
            "object size verified."
        ),
    }


def upload_metadata_file(
    bucket: storage.Bucket,
    local_path: Path,
    object_name: str,
) -> None:
    """Upload a small pipeline metadata file."""

    blob = bucket.blob(
        object_name
    )

    blob.upload_from_filename(
        str(local_path),
        checksum="auto",
    )

    print(
        f"Metadata uploaded: "
        f"gs://{bucket.name}/"
        f"{object_name}"
    )


def save_upload_reports(
    results: pd.DataFrame,
    bucket_name: str,
    project_id: str,
) -> None:
    """Save local cloud-ingestion reports."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = (
        OUTPUT_DIRECTORY
        / "gcs_upload_report.csv"
    )

    summary_path = (
        OUTPUT_DIRECTORY
        / "gcs_upload_summary.json"
    )

    results.to_csv(
        report_path,
        index=False,
        encoding="utf-8",
    )

    status_counts = (
        results[
            "status"
        ]
        .value_counts()
        .to_dict()
    )

    successful_statuses = {
        "UPLOADED",
        "SKIPPED",
    }

    successful_batches = int(
        results[
            "status"
        ]
        .isin(successful_statuses)
        .sum()
    )

    summary = {
        "project_id": project_id,
        "bucket_name": bucket_name,
        "upload_timestamp_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "batch_count": int(
            len(results)
        ),
        "successful_batches": (
            successful_batches
        ),
        "status_counts": status_counts,
        "total_rows": int(
            results[
                "row_count"
            ].sum()
        ),
        "total_bytes": int(
            results[
                "file_size_bytes"
            ].sum()
        ),
        "upload_success": (
            successful_batches
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

    print("\nCreated upload reports:")
    print(f"- {report_path}")
    print(f"- {summary_path}")


def main() -> None:
    print(
        "Starting validated Cloud Storage upload."
    )

    (
        project_id,
        region,
        bucket_name,
    ) = load_configuration()

    validation_batches = (
        verify_local_validation()
    )

    manifest = (
        load_batch_manifest()
    )

    validated_batch_ids = set(
        validation_batches[
            "dataset_name"
        ].astype(str)
    )

    manifest = manifest[
        manifest[
            "batch_id"
        ]
        .astype(str)
        .isin(
            validated_batch_ids
        )
    ].copy()

    storage_client = storage.Client(
        project=project_id
    )

    bucket = verify_bucket(
        client=storage_client,
        bucket_name=bucket_name,
        expected_region=region,
    )

    print("\nUploading clean batches")
    print("-----------------------")

    upload_results = []

    for _, batch_row in (
        manifest.sort_values(
            "batch_sequence"
        ).iterrows()
    ):
        result = upload_single_batch(
            bucket=bucket,
            batch_row=batch_row,
        )

        upload_results.append(
            result
        )

        print(
            f"{result['batch_id']}: "
            f"{result['status']}"
        )

    results_dataframe = pd.DataFrame(
        upload_results
    )

    failed_results = (
        results_dataframe[
            ~results_dataframe[
                "status"
            ].isin(
                [
                    "UPLOADED",
                    "SKIPPED",
                ]
            )
        ]
    )

    if not failed_results.empty:
        save_upload_reports(
            results=results_dataframe,
            bucket_name=bucket_name,
            project_id=project_id,
        )

        raise RuntimeError(
            "One or more batch uploads failed "
            "or produced a conflict."
        )

    print(
        "\nUploading pipeline manifests"
    )
    print(
        "----------------------------"
    )

    upload_metadata_file(
        bucket=bucket,
        local_path=BATCH_MANIFEST_PATH,
        object_name=(
            "manifests/"
            "batch_manifest.csv"
        ),
    )

    upload_metadata_file(
        bucket=bucket,
        local_path=(
            VALIDATION_BATCH_SUMMARY_PATH
        ),
        object_name=(
            "manifests/"
            "validation_batch_summary.csv"
        ),
    )

    save_upload_reports(
        results=results_dataframe,
        bucket_name=bucket_name,
        project_id=project_id,
    )

    uploaded_count = int(
        (
            results_dataframe[
                "status"
            ]
            == "UPLOADED"
        ).sum()
    )

    skipped_count = int(
        (
            results_dataframe[
                "status"
            ]
            == "SKIPPED"
        ).sum()
    )

    print("\nGCS upload summary")
    print("------------------")

    print(
        f"Batches processed: "
        f"{len(results_dataframe)}"
    )

    print(
        f"Uploaded: "
        f"{uploaded_count}"
    )

    print(
        f"Already present / skipped: "
        f"{skipped_count}"
    )

    print(
        f"Total rows represented: "
        f"{int(results_dataframe['row_count'].sum()):,}"
    )

    print(
        "\nGCS UPLOAD COMPLETED SUCCESSFULLY."
    )


if __name__ == "__main__":
    main()