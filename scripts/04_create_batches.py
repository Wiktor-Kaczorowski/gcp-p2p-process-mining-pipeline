from __future__ import annotations

import hashlib
import json
import math
import shutil
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CANONICAL_DATASET = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "canonical_events"
)

BATCH_OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "batches"
    / "clean"
)

ARTIFACT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "batches"
)

BATCH_SIZE = 100_000

# These dates are synthetic technical ingestion dates.
# They do NOT represent the original business event dates.
SIMULATED_START_DATE = pd.Timestamp(
    "2026-01-01",
    tz="UTC",
)


def load_canonical_events() -> pd.DataFrame:
    """Load the complete canonical event dataset."""

    if not CANONICAL_DATASET.exists():
        raise FileNotFoundError(
            "Canonical dataset not found.\n"
            f"Expected location: {CANONICAL_DATASET}"
        )

    print("Loading canonical event dataset.")

    dataframe = pd.read_parquet(
        CANONICAL_DATASET
    )

    if dataframe.empty:
        raise ValueError(
            "Canonical event dataset is empty."
        )

    print(
        f"Loaded {len(dataframe):,} canonical events."
    )

    return dataframe


def validate_canonical_dataset(
    dataframe: pd.DataFrame,
) -> None:
    """Validate fields required for batch generation."""

    required_columns = {
        "event_id",
        "case_id",
        "activity",
        "event_timestamp",
        "source_row_number",
        "timestamp_quality_status",
    }

    missing_columns = required_columns.difference(
        dataframe.columns
    )

    if missing_columns:
        raise ValueError(
            "Canonical dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if dataframe["event_id"].isna().any():
        raise ValueError(
            "event_id contains missing values."
        )

    if dataframe["event_id"].duplicated().any():
        raise ValueError(
            "event_id is not unique."
        )


def prepare_batch_output_directory() -> None:
    """Remove old clean batches and create output directories."""

    if BATCH_OUTPUT_DIRECTORY.exists():
        shutil.rmtree(
            BATCH_OUTPUT_DIRECTORY
        )

    BATCH_OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    ARTIFACT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )


def calculate_sha256(file_path: Path) -> str:
    """Calculate SHA-256 checksum of a file."""

    sha256_hash = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)

            if not chunk:
                break

            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()


def timestamp_to_string(
    value: Any,
) -> str | None:
    """Convert timestamps into JSON/CSV-safe strings."""

    if value is None or pd.isna(value):
        return None

    return pd.Timestamp(value).isoformat()


def create_batches(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Split canonical events into deterministic ingestion batches."""

    print("Sorting events chronologically.")

    dataframe = dataframe.sort_values(
        by=[
            "event_timestamp",
            "source_row_number",
        ],
        kind="mergesort",
    ).reset_index(drop=True)

    total_rows = len(dataframe)

    batch_count = math.ceil(
        total_rows / BATCH_SIZE
    )

    print(
        f"Creating {batch_count} batches "
        f"with maximum size {BATCH_SIZE:,}."
    )

    manifest_rows: list[dict[str, Any]] = []

    for batch_sequence in range(
        1,
        batch_count + 1,
    ):
        start_index = (
            batch_sequence - 1
        ) * BATCH_SIZE

        end_index = min(
            batch_sequence * BATCH_SIZE,
            total_rows,
        )

        batch = dataframe.iloc[
            start_index:end_index
        ].copy()

        batch_id = (
            f"batch_{batch_sequence:04d}"
        )

        simulated_ingestion_timestamp = (
            SIMULATED_START_DATE
            + pd.Timedelta(
                days=batch_sequence - 1,
                hours=8,
            )
        )

        simulated_ingestion_date = (
            simulated_ingestion_timestamp.date()
        )

        batch["ingestion_batch_id"] = (
            batch_id
        )

        batch["ingestion_batch_sequence"] = (
            batch_sequence
        )

        batch["simulated_ingestion_timestamp_utc"] = (
            simulated_ingestion_timestamp
        )

        batch["simulated_ingestion_date"] = (
            simulated_ingestion_date
        )

        batch["batch_row_number"] = range(
            1,
            len(batch) + 1,
        )

        batch_directory = (
            BATCH_OUTPUT_DIRECTORY
            / batch_id
        )

        batch_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            batch_directory
            / "events.parquet"
        )

        batch.to_parquet(
            output_path,
            index=False,
            compression="snappy",
        )

        file_size_bytes = (
            output_path.stat().st_size
        )

        checksum = calculate_sha256(
            output_path
        )

        historical_outlier_count = int(
            (
                batch[
                    "timestamp_quality_status"
                ]
                == "historical_outlier"
            ).sum()
        )

        manifest_rows.append(
            {
                "batch_id": batch_id,
                "batch_sequence": batch_sequence,
                "simulated_ingestion_date": (
                    simulated_ingestion_date.isoformat()
                ),
                "row_count": int(
                    len(batch)
                ),
                "unique_case_count": int(
                    batch[
                        "case_id"
                    ].nunique()
                ),
                "unique_activity_count": int(
                    batch[
                        "activity"
                    ].nunique()
                ),
                "minimum_event_timestamp": (
                    timestamp_to_string(
                        batch[
                            "event_timestamp"
                        ].min()
                    )
                ),
                "maximum_event_timestamp": (
                    timestamp_to_string(
                        batch[
                            "event_timestamp"
                        ].max()
                    )
                ),
                "historical_outlier_count": (
                    historical_outlier_count
                ),
                "file_name": (
                    str(
                        output_path.relative_to(
                            PROJECT_ROOT
                        )
                    )
                ),
                "file_size_bytes": int(
                    file_size_bytes
                ),
                "sha256": checksum,
                "status": "READY",
            }
        )

        print(
            f"{batch_id}: "
            f"{len(batch):,} rows | "
            f"{batch['case_id'].nunique():,} cases"
        )

    return pd.DataFrame(
        manifest_rows
    )


def validate_batches(
    manifest: pd.DataFrame,
    expected_row_count: int,
) -> None:
    """Validate generated batches."""

    actual_row_count = int(
        manifest["row_count"].sum()
    )

    if actual_row_count != expected_row_count:
        raise ValueError(
            "Batch row-count validation failed. "
            f"Expected {expected_row_count:,}, "
            f"received {actual_row_count:,}."
        )

    if manifest["batch_id"].duplicated().any():
        raise ValueError(
            "Duplicate batch identifiers detected."
        )

    missing_files = []

    for file_name in manifest["file_name"]:
        file_path = (
            PROJECT_ROOT
            / file_name
        )

        if not file_path.exists():
            missing_files.append(
                str(file_path)
            )

    if missing_files:
        raise FileNotFoundError(
            "Some generated batch files are missing:\n"
            + "\n".join(
                missing_files
            )
        )


def save_manifest(
    manifest: pd.DataFrame,
    canonical_dataframe: pd.DataFrame,
) -> None:
    """Save batch manifest and summary."""

    manifest_path = (
        ARTIFACT_DIRECTORY
        / "batch_manifest.csv"
    )

    summary_path = (
        ARTIFACT_DIRECTORY
        / "batch_generation_summary.json"
    )

    manifest.to_csv(
        manifest_path,
        index=False,
        encoding="utf-8",
    )

    summary = {
        "batch_count": int(
            len(manifest)
        ),
        "configured_batch_size": int(
            BATCH_SIZE
        ),
        "total_row_count": int(
            manifest["row_count"].sum()
        ),
        "canonical_case_count": int(
            canonical_dataframe[
                "case_id"
            ].nunique()
        ),
        "canonical_activity_count": int(
            canonical_dataframe[
                "activity"
            ].nunique()
        ),
        "first_simulated_ingestion_date": (
            manifest[
                "simulated_ingestion_date"
            ].min()
        ),
        "last_simulated_ingestion_date": (
            manifest[
                "simulated_ingestion_date"
            ].max()
        ),
        "smallest_batch_row_count": int(
            manifest[
                "row_count"
            ].min()
        ),
        "largest_batch_row_count": int(
            manifest[
                "row_count"
            ].max()
        ),
        "batch_strategy": (
            "Chronological event ordering with "
            "fixed-size simulated ingestion batches."
        ),
        "important_note": (
            "The simulated ingestion date is technical "
            "metadata and does not replace the original "
            "business event timestamp."
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

    print("\nCreated batch metadata:")
    print(f"- {manifest_path}")
    print(f"- {summary_path}")


def print_summary(
    manifest: pd.DataFrame,
) -> None:
    """Print final generation summary."""

    print("\nBatch generation summary")
    print("------------------------")
    print(
        f"Batches: {len(manifest):,}"
    )

    print(
        "Total rows: "
        f"{manifest['row_count'].sum():,}"
    )

    print(
        "First batch: "
        f"{manifest.iloc[0]['batch_id']} | "
        f"{manifest.iloc[0]['row_count']:,} rows"
    )

    print(
        "Last batch: "
        f"{manifest.iloc[-1]['batch_id']} | "
        f"{manifest.iloc[-1]['row_count']:,} rows"
    )


def main() -> None:
    dataframe = load_canonical_events()

    validate_canonical_dataset(
        dataframe
    )

    prepare_batch_output_directory()

    manifest = create_batches(
        dataframe
    )

    validate_batches(
        manifest=manifest,
        expected_row_count=len(dataframe),
    )

    save_manifest(
        manifest=manifest,
        canonical_dataframe=dataframe,
    )

    print_summary(
        manifest
    )

    print(
        "\nClean batch generation "
        "completed successfully."
    )


if __name__ == "__main__":
    main()