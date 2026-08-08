from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEAN_BATCH_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "batches"
    / "clean"
)

INVALID_BATCH_DIRECTORY = (
    PROJECT_ROOT
    / "data"
    / "batches"
    / "invalid"
)

ARTIFACT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "data_quality"
)


def get_clean_batch_path(
    batch_id: str,
) -> Path:
    """Return the path of a clean batch."""

    batch_path = (
        CLEAN_BATCH_DIRECTORY
        / batch_id
        / "events.parquet"
    )

    if not batch_path.exists():
        raise FileNotFoundError(
            f"Clean batch does not exist: {batch_path}"
        )

    return batch_path


def load_clean_batch(
    batch_id: str,
) -> pd.DataFrame:
    """Load a clean batch."""

    batch_path = get_clean_batch_path(
        batch_id
    )

    return pd.read_parquet(
        batch_path
    )


def calculate_sha256(
    file_path: Path,
) -> str:
    """Calculate SHA-256 checksum."""

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


def prepare_output_directories() -> None:
    """Clear previous invalid scenarios."""

    if INVALID_BATCH_DIRECTORY.exists():
        shutil.rmtree(
            INVALID_BATCH_DIRECTORY
        )

    INVALID_BATCH_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    ARTIFACT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )


def save_invalid_batch(
    dataframe: pd.DataFrame,
    scenario_name: str,
) -> Path:
    """Write a modified batch into the quarantine test area."""

    output_directory = (
        INVALID_BATCH_DIRECTORY
        / scenario_name
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / "events.parquet"
    )

    dataframe.to_parquet(
        output_path,
        index=False,
        compression="snappy",
    )

    return output_path


def build_manifest_row(
    scenario_name: str,
    source_batch_id: str,
    expected_issue: str,
    modified_rows: int | None,
    output_path: Path,
) -> dict[str, Any]:
    """Create one scenario-manifest row."""

    return {
        "scenario_name": scenario_name,
        "source_batch_id": source_batch_id,
        "expected_issue": expected_issue,
        "modified_rows": modified_rows,
        "output_file": str(
            output_path.relative_to(
                PROJECT_ROOT
            )
        ),
        "file_size_bytes": int(
            output_path.stat().st_size
        ),
        "sha256": calculate_sha256(
            output_path
        ),
    }


def create_duplicate_events_scenario() -> dict[str, Any]:
    """Append exact duplicate event records."""

    source_batch_id = "batch_0003"

    dataframe = load_clean_batch(
        source_batch_id
    )

    duplicate_count = 250

    duplicates = dataframe.head(
        duplicate_count
    ).copy()

    invalid_dataframe = pd.concat(
        [
            dataframe,
            duplicates,
        ],
        ignore_index=True,
    )

    output_path = save_invalid_batch(
        dataframe=invalid_dataframe,
        scenario_name="duplicate_events",
    )

    return build_manifest_row(
        scenario_name="duplicate_events",
        source_batch_id=source_batch_id,
        expected_issue=(
            "Duplicate event_id values"
        ),
        modified_rows=duplicate_count,
        output_path=output_path,
    )


def create_missing_case_id_scenario() -> dict[str, Any]:
    """Remove case identifiers from selected events."""

    source_batch_id = "batch_0005"

    dataframe = load_clean_batch(
        source_batch_id
    )

    modified_rows = 100

    dataframe.loc[
        dataframe.index[:modified_rows],
        "case_id",
    ] = pd.NA

    output_path = save_invalid_batch(
        dataframe=dataframe,
        scenario_name="missing_case_id",
    )

    return build_manifest_row(
        scenario_name="missing_case_id",
        source_batch_id=source_batch_id,
        expected_issue=(
            "Missing mandatory case_id"
        ),
        modified_rows=modified_rows,
        output_path=output_path,
    )


def create_missing_timestamp_scenario() -> dict[str, Any]:
    """Remove timestamps from selected events."""

    source_batch_id = "batch_0007"

    dataframe = load_clean_batch(
        source_batch_id
    )

    modified_rows = 50

    dataframe.loc[
        dataframe.index[:modified_rows],
        "event_timestamp",
    ] = pd.NaT

    output_path = save_invalid_batch(
        dataframe=dataframe,
        scenario_name="missing_timestamp",
    )

    return build_manifest_row(
        scenario_name="missing_timestamp",
        source_batch_id=source_batch_id,
        expected_issue=(
            "Missing mandatory event_timestamp"
        ),
        modified_rows=modified_rows,
        output_path=output_path,
    )


def create_schema_drift_scenario() -> dict[str, Any]:
    """Simulate a source-system column rename."""

    source_batch_id = "batch_0009"

    dataframe = load_clean_batch(
        source_batch_id
    )

    dataframe = dataframe.rename(
        columns={
            "activity": "activity_name",
        }
    )

    output_path = save_invalid_batch(
        dataframe=dataframe,
        scenario_name="schema_drift",
    )

    return build_manifest_row(
        scenario_name="schema_drift",
        source_batch_id=source_batch_id,
        expected_issue=(
            "Required activity column renamed "
            "to activity_name"
        ),
        modified_rows=len(dataframe),
        output_path=output_path,
    )


def create_duplicate_delivery_scenario() -> dict[str, Any]:
    """
    Copy a previously valid batch without changing its batch ID.

    This simulates receiving the same source batch twice.
    """

    source_batch_id = "batch_0011"

    dataframe = load_clean_batch(
        source_batch_id
    )

    output_path = save_invalid_batch(
        dataframe=dataframe,
        scenario_name="duplicate_batch_delivery",
    )

    return build_manifest_row(
        scenario_name="duplicate_batch_delivery",
        source_batch_id=source_batch_id,
        expected_issue=(
            "Previously processed ingestion_batch_id"
        ),
        modified_rows=0,
        output_path=output_path,
    )


def create_corrupted_parquet_scenario() -> dict[str, Any]:
    """Create a physically corrupted Parquet file."""

    source_batch_id = "batch_0012"

    source_path = get_clean_batch_path(
        source_batch_id
    )

    scenario_name = (
        "corrupted_parquet"
    )

    output_directory = (
        INVALID_BATCH_DIRECTORY
        / scenario_name
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_directory
        / "events.parquet"
    )

    shutil.copy2(
        source_path,
        output_path,
    )

    # Parquet metadata/footer is stored at the end of the file.
    # Removing bytes from the end intentionally damages the file.
    with output_path.open(
        "r+b"
    ) as file:
        file.seek(
            0,
            2,
        )

        file_size = file.tell()

        bytes_to_remove = min(
            4096,
            file_size // 10,
        )

        file.truncate(
            file_size
            - bytes_to_remove
        )

    return build_manifest_row(
        scenario_name=scenario_name,
        source_batch_id=source_batch_id,
        expected_issue=(
            "Unreadable corrupted Parquet file"
        ),
        modified_rows=None,
        output_path=output_path,
    )


def save_scenario_manifest(
    manifest: pd.DataFrame,
) -> None:
    """Save metadata describing all controlled issues."""

    manifest_path = (
        ARTIFACT_DIRECTORY
        / "invalid_batch_scenarios.csv"
    )

    summary_path = (
        ARTIFACT_DIRECTORY
        / "invalid_batch_scenarios_summary.json"
    )

    manifest.to_csv(
        manifest_path,
        index=False,
        encoding="utf-8",
    )

    summary = {
        "scenario_count": int(
            len(manifest)
        ),
        "scenarios": manifest[
            "scenario_name"
        ].tolist(),
        "purpose": (
            "Controlled failure scenarios for "
            "data-quality and ingestion testing."
        ),
        "clean_batches_modified": False,
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

    print("\nCreated metadata:")
    print(f"- {manifest_path}")
    print(f"- {summary_path}")


def main() -> None:
    prepare_output_directories()

    print(
        "Creating controlled data-quality scenarios."
    )

    scenarios = [
        create_duplicate_events_scenario(),
        create_missing_case_id_scenario(),
        create_missing_timestamp_scenario(),
        create_schema_drift_scenario(),
        create_duplicate_delivery_scenario(),
        create_corrupted_parquet_scenario(),
    ]

    manifest = pd.DataFrame(
        scenarios
    )

    save_scenario_manifest(
        manifest
    )

    print(
        "\nControlled failure scenarios"
    )
    print(
        "----------------------------"
    )

    for scenario in scenarios:
        print(
            f"{scenario['scenario_name']}: "
            f"{scenario['expected_issue']}"
        )

    print(
        "\nInvalid test-batch generation "
        "completed successfully."
    )


if __name__ == "__main__":
    main()