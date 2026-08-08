from __future__ import annotations

import json
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

BATCH_MANIFEST_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "batches"
    / "batch_manifest.csv"
)

INVALID_SCENARIOS_PATH = (
    PROJECT_ROOT
    / "artifacts"
    / "data_quality"
    / "invalid_batch_scenarios.csv"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "data_quality"
)


MANDATORY_COLUMNS = [
    "event_id",
    "case_id",
    "activity",
    "event_timestamp",
    "ingestion_batch_id",
]


EXPECTED_SCENARIO_RULES = {
    "duplicate_events": "DQ_UNIQUE_EVENT_ID",
    "missing_case_id": "DQ_NOT_NULL_CASE_ID",
    "missing_timestamp": "DQ_NOT_NULL_EVENT_TIMESTAMP",
    "schema_drift": "DQ_SCHEMA_COLUMNS",
    "duplicate_batch_delivery": "DQ_IDEMPOTENCY",
    "corrupted_parquet": "FILE_READABLE",
}


def create_result(
    dataset_name: str,
    batch_id: str | None,
    rule_id: str,
    status: str,
    failed_rows: int = 0,
    message: str = "",
) -> dict[str, Any]:
    """Create one validation-result record."""

    return {
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "rule_id": rule_id,
        "status": status,
        "failed_rows": int(failed_rows),
        "message": message,
    }


def load_batch_manifest() -> pd.DataFrame:
    """Load metadata describing clean batches."""

    if not BATCH_MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Batch manifest not found: {BATCH_MANIFEST_PATH}"
        )

    return pd.read_csv(
        BATCH_MANIFEST_PATH
    )


def load_invalid_scenarios() -> pd.DataFrame:
    """Load controlled failure-scenario metadata."""

    if not INVALID_SCENARIOS_PATH.exists():
        raise FileNotFoundError(
            "Invalid scenario manifest not found: "
            f"{INVALID_SCENARIOS_PATH}"
        )

    return pd.read_csv(
        INVALID_SCENARIOS_PATH
    )


def get_baseline_schema() -> set[str]:
    """
    Use a clean batch as the expected input schema.

    This avoids manually duplicating the complete list
    of expected columns.
    """

    baseline_path = (
        CLEAN_BATCH_DIRECTORY
        / "batch_0001"
        / "events.parquet"
    )

    if not baseline_path.exists():
        raise FileNotFoundError(
            f"Baseline batch not found: {baseline_path}"
        )

    dataframe = pd.read_parquet(
        baseline_path
    )

    return set(
        dataframe.columns
    )


def safely_read_parquet(
    file_path: Path,
    dataset_name: str,
    expected_batch_id: str | None,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    """Read a Parquet file and convert read failures into DQ results."""

    try:
        dataframe = pd.read_parquet(
            file_path
        )

        result = create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="FILE_READABLE",
            status="PASS",
            message="Parquet file opened successfully.",
        )

        return dataframe, result

    except Exception as exc:
        result = create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="FILE_READABLE",
            status="FAIL",
            failed_rows=1,
            message=(
                f"{type(exc).__name__}: {str(exc)[:500]}"
            ),
        )

        return None, result


def validate_schema(
    dataframe: pd.DataFrame,
    expected_schema: set[str],
    dataset_name: str,
    batch_id: str | None,
) -> dict[str, Any]:
    """Validate the complete expected column set."""

    actual_schema = set(
        dataframe.columns
    )

    missing_columns = sorted(
        expected_schema - actual_schema
    )

    unexpected_columns = sorted(
        actual_schema - expected_schema
    )

    if missing_columns or unexpected_columns:
        message_parts = []

        if missing_columns:
            message_parts.append(
                "Missing columns: "
                + ", ".join(missing_columns)
            )

        if unexpected_columns:
            message_parts.append(
                "Unexpected columns: "
                + ", ".join(unexpected_columns)
            )

        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id="DQ_SCHEMA_COLUMNS",
            status="FAIL",
            failed_rows=len(
                missing_columns
            ) + len(
                unexpected_columns
            ),
            message=" | ".join(
                message_parts
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=batch_id,
        rule_id="DQ_SCHEMA_COLUMNS",
        status="PASS",
        message="Schema matches baseline.",
    )


def validate_not_null(
    dataframe: pd.DataFrame,
    column: str,
    rule_id: str,
    dataset_name: str,
    batch_id: str | None,
) -> dict[str, Any]:
    """Validate that a mandatory field is populated."""

    if column not in dataframe.columns:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id=rule_id,
            status="SKIPPED",
            message=(
                f"Column '{column}' is unavailable "
                "because schema validation failed."
            ),
        )

    failed_rows = int(
        dataframe[column]
        .isna()
        .sum()
    )

    if failed_rows > 0:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id=rule_id,
            status="FAIL",
            failed_rows=failed_rows,
            message=(
                f"{failed_rows:,} rows contain "
                f"NULL in {column}."
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=batch_id,
        rule_id=rule_id,
        status="PASS",
        message=(
            f"Column '{column}' contains no NULL values."
        ),
    )


def validate_unique_event_id(
    dataframe: pd.DataFrame,
    dataset_name: str,
    batch_id: str | None,
) -> dict[str, Any]:
    """Validate uniqueness of event identifiers."""

    if "event_id" not in dataframe.columns:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id="DQ_UNIQUE_EVENT_ID",
            status="SKIPPED",
            message="event_id column is unavailable.",
        )

    duplicate_count = int(
        dataframe["event_id"]
        .duplicated()
        .sum()
    )

    if duplicate_count > 0:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id="DQ_UNIQUE_EVENT_ID",
            status="FAIL",
            failed_rows=duplicate_count,
            message=(
                f"{duplicate_count:,} duplicate "
                "event_id values detected."
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=batch_id,
        rule_id="DQ_UNIQUE_EVENT_ID",
        status="PASS",
        message="event_id is unique.",
    )


def validate_row_count(
    dataframe: pd.DataFrame,
    expected_row_count: int | None,
    dataset_name: str,
    batch_id: str | None,
) -> dict[str, Any]:
    """Compare actual row count with batch manifest."""

    if expected_row_count is None:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id="DQ_ROW_COUNT",
            status="SKIPPED",
            message="No expected row count is available.",
        )

    actual_row_count = len(
        dataframe
    )

    difference = (
        actual_row_count
        - expected_row_count
    )

    if difference != 0:
        return create_result(
            dataset_name=dataset_name,
            batch_id=batch_id,
            rule_id="DQ_ROW_COUNT",
            status="FAIL",
            failed_rows=abs(
                difference
            ),
            message=(
                f"Expected {expected_row_count:,} rows, "
                f"received {actual_row_count:,}."
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=batch_id,
        rule_id="DQ_ROW_COUNT",
        status="PASS",
        message=(
            f"Row count matches expected "
            f"value: {expected_row_count:,}."
        ),
    )


def validate_batch_id(
    dataframe: pd.DataFrame,
    expected_batch_id: str | None,
    dataset_name: str,
) -> dict[str, Any]:
    """Check consistency of ingestion_batch_id."""

    if "ingestion_batch_id" not in dataframe.columns:
        return create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="DQ_BATCH_ID",
            status="SKIPPED",
            message="ingestion_batch_id column is unavailable.",
        )

    values = (
        dataframe[
            "ingestion_batch_id"
        ]
        .dropna()
        .unique()
        .tolist()
    )

    if len(values) != 1:
        return create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="DQ_BATCH_ID",
            status="FAIL",
            failed_rows=len(dataframe),
            message=(
                "Batch contains multiple or zero "
                "ingestion_batch_id values."
            ),
        )

    actual_batch_id = str(
        values[0]
    )

    if (
        expected_batch_id is not None
        and actual_batch_id != expected_batch_id
    ):
        return create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="DQ_BATCH_ID",
            status="FAIL",
            failed_rows=len(dataframe),
            message=(
                f"Expected {expected_batch_id}, "
                f"received {actual_batch_id}."
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=expected_batch_id,
        rule_id="DQ_BATCH_ID",
        status="PASS",
        message=(
            f"Batch identifier is {actual_batch_id}."
        ),
    )


def validate_idempotency(
    expected_batch_id: str | None,
    dataset_name: str,
    already_processed_batch_ids: set[str],
) -> dict[str, Any]:
    """Check whether the batch was already processed."""

    if expected_batch_id is None:
        return create_result(
            dataset_name=dataset_name,
            batch_id=None,
            rule_id="DQ_IDEMPOTENCY",
            status="SKIPPED",
            message="Expected batch ID is unavailable.",
        )

    if expected_batch_id in already_processed_batch_ids:
        return create_result(
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
            rule_id="DQ_IDEMPOTENCY",
            status="FAIL",
            failed_rows=1,
            message=(
                f"{expected_batch_id} has already "
                "been processed."
            ),
        )

    return create_result(
        dataset_name=dataset_name,
        batch_id=expected_batch_id,
        rule_id="DQ_IDEMPOTENCY",
        status="PASS",
        message=(
            f"{expected_batch_id} has not previously "
            "been processed."
        ),
    )


def validate_single_batch(
    file_path: Path,
    dataset_name: str,
    expected_batch_id: str,
    expected_row_count: int | None,
    expected_schema: set[str],
    already_processed_batch_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Run the complete validation suite against one batch."""

    if already_processed_batch_ids is None:
        already_processed_batch_ids = set()

    results: list[dict[str, Any]] = []

    dataframe, read_result = safely_read_parquet(
        file_path=file_path,
        dataset_name=dataset_name,
        expected_batch_id=expected_batch_id,
    )

    results.append(
        read_result
    )

    if dataframe is None:
        return results

    results.append(
        validate_schema(
            dataframe=dataframe,
            expected_schema=expected_schema,
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_not_null(
            dataframe=dataframe,
            column="case_id",
            rule_id="DQ_NOT_NULL_CASE_ID",
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_not_null(
            dataframe=dataframe,
            column="activity",
            rule_id="DQ_NOT_NULL_ACTIVITY",
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_not_null(
            dataframe=dataframe,
            column="event_timestamp",
            rule_id="DQ_NOT_NULL_EVENT_TIMESTAMP",
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_unique_event_id(
            dataframe=dataframe,
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_row_count(
            dataframe=dataframe,
            expected_row_count=expected_row_count,
            dataset_name=dataset_name,
            batch_id=expected_batch_id,
        )
    )

    results.append(
        validate_batch_id(
            dataframe=dataframe,
            expected_batch_id=expected_batch_id,
            dataset_name=dataset_name,
        )
    )

    results.append(
        validate_idempotency(
            expected_batch_id=expected_batch_id,
            dataset_name=dataset_name,
            already_processed_batch_ids=(
                already_processed_batch_ids
            ),
        )
    )

    return results


def validate_clean_batches(
    manifest: pd.DataFrame,
    expected_schema: set[str],
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    """Validate every clean ingestion batch."""

    print("\nValidating clean batches")
    print("------------------------")

    all_results: list[dict[str, Any]] = []
    summary_rows = []

    for _, row in manifest.iterrows():
        batch_id = str(
            row["batch_id"]
        )

        expected_row_count = int(
            row["row_count"]
        )

        file_path = (
            CLEAN_BATCH_DIRECTORY
            / batch_id
            / "events.parquet"
        )

        results = validate_single_batch(
            file_path=file_path,
            dataset_name=batch_id,
            expected_batch_id=batch_id,
            expected_row_count=expected_row_count,
            expected_schema=expected_schema,
        )

        all_results.extend(
            results
        )

        failed_rules = [
            result["rule_id"]
            for result in results
            if result["status"] == "FAIL"
        ]

        overall_status = (
            "PASS"
            if not failed_rules
            else "FAIL"
        )

        summary_rows.append(
            {
                "dataset_name": batch_id,
                "dataset_type": "clean",
                "overall_status": overall_status,
                "failed_rule_count": len(
                    failed_rules
                ),
                "failed_rules": ", ".join(
                    failed_rules
                ),
            }
        )

        print(
            f"{batch_id}: {overall_status}"
        )

    return (
        all_results,
        pd.DataFrame(summary_rows),
    )


def validate_invalid_scenarios(
    scenario_manifest: pd.DataFrame,
    clean_manifest: pd.DataFrame,
    expected_schema: set[str],
) -> tuple[
    list[dict[str, Any]],
    pd.DataFrame,
]:
    """Validate controlled failure scenarios."""

    print("\nValidating controlled failure scenarios")
    print("----------------------------------------")

    all_results: list[dict[str, Any]] = []
    summary_rows = []

    expected_row_counts = dict(
        zip(
            clean_manifest["batch_id"],
            clean_manifest["row_count"],
        )
    )

    for _, row in scenario_manifest.iterrows():
        scenario_name = str(
            row["scenario_name"]
        )

        source_batch_id = str(
            row["source_batch_id"]
        )

        file_path = (
            INVALID_BATCH_DIRECTORY
            / scenario_name
            / "events.parquet"
        )

        already_processed: set[str] = set()

        # This scenario specifically simulates receiving
        # a batch that has already been successfully loaded.
        if scenario_name == "duplicate_batch_delivery":
            already_processed.add(
                source_batch_id
            )

        results = validate_single_batch(
            file_path=file_path,
            dataset_name=scenario_name,
            expected_batch_id=source_batch_id,
            expected_row_count=int(
                expected_row_counts[
                    source_batch_id
                ]
            ),
            expected_schema=expected_schema,
            already_processed_batch_ids=already_processed,
        )

        all_results.extend(
            results
        )

        failed_rules = [
            result["rule_id"]
            for result in results
            if result["status"] == "FAIL"
        ]

        expected_rule = (
            EXPECTED_SCENARIO_RULES[
                scenario_name
            ]
        )

        expected_failure_detected = (
            expected_rule
            in failed_rules
        )

        summary_rows.append(
            {
                "dataset_name": scenario_name,
                "dataset_type": "controlled_failure",
                "source_batch_id": source_batch_id,
                "overall_status": (
                    "FAIL"
                    if failed_rules
                    else "PASS"
                ),
                "expected_rule": expected_rule,
                "expected_failure_detected": (
                    expected_failure_detected
                ),
                "failed_rule_count": len(
                    failed_rules
                ),
                "failed_rules": ", ".join(
                    failed_rules
                ),
            }
        )

        detection_status = (
            "DETECTED"
            if expected_failure_detected
            else "NOT DETECTED"
        )

        print(
            f"{scenario_name}: "
            f"{detection_status} | "
            f"{', '.join(failed_rules)}"
        )

    return (
        all_results,
        pd.DataFrame(summary_rows),
    )


def save_results(
    rule_results: pd.DataFrame,
    batch_summary: pd.DataFrame,
    scenario_summary: pd.DataFrame,
) -> None:
    """Save validator reports."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    rule_results_path = (
        OUTPUT_DIRECTORY
        / "validation_rule_results.csv"
    )

    batch_summary_path = (
        OUTPUT_DIRECTORY
        / "validation_batch_summary.csv"
    )

    scenario_summary_path = (
        OUTPUT_DIRECTORY
        / "scenario_detection_results.csv"
    )

    overall_summary_path = (
        OUTPUT_DIRECTORY
        / "validation_summary.json"
    )

    rule_results.to_csv(
        rule_results_path,
        index=False,
        encoding="utf-8",
    )

    batch_summary.to_csv(
        batch_summary_path,
        index=False,
        encoding="utf-8",
    )

    scenario_summary.to_csv(
        scenario_summary_path,
        index=False,
        encoding="utf-8",
    )

    clean_batches_passed = int(
        (
            batch_summary[
                "overall_status"
            ]
            == "PASS"
        ).sum()
    )

    scenarios_detected = int(
        scenario_summary[
            "expected_failure_detected"
        ].sum()
    )

    summary = {
        "clean_batches_tested": int(
            len(batch_summary)
        ),
        "clean_batches_passed": (
            clean_batches_passed
        ),
        "controlled_scenarios_tested": int(
            len(scenario_summary)
        ),
        "controlled_scenarios_detected": (
            scenarios_detected
        ),
        "total_validation_rule_results": int(
            len(rule_results)
        ),
        "clean_validation_success": (
            clean_batches_passed
            == len(batch_summary)
        ),
        "failure_detection_success": (
            scenarios_detected
            == len(scenario_summary)
        ),
    }

    with overall_summary_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print("\nCreated validation reports:")
    print(f"- {rule_results_path}")
    print(f"- {batch_summary_path}")
    print(f"- {scenario_summary_path}")
    print(f"- {overall_summary_path}")


def main() -> None:
    print(
        "Starting batch data-quality validation."
    )

    clean_manifest = (
        load_batch_manifest()
    )

    scenario_manifest = (
        load_invalid_scenarios()
    )

    expected_schema = (
        get_baseline_schema()
    )

    clean_results, clean_summary = (
        validate_clean_batches(
            manifest=clean_manifest,
            expected_schema=expected_schema,
        )
    )

    invalid_results, scenario_summary = (
        validate_invalid_scenarios(
            scenario_manifest=scenario_manifest,
            clean_manifest=clean_manifest,
            expected_schema=expected_schema,
        )
    )

    all_results = pd.DataFrame(
        clean_results
        + invalid_results
    )

    save_results(
        rule_results=all_results,
        batch_summary=clean_summary,
        scenario_summary=scenario_summary,
    )

    clean_passed = int(
        (
            clean_summary[
                "overall_status"
            ]
            == "PASS"
        ).sum()
    )

    detected = int(
        scenario_summary[
            "expected_failure_detected"
        ].sum()
    )

    print("\nValidation suite summary")
    print("------------------------")

    print(
        f"Clean batches passed: "
        f"{clean_passed}/"
        f"{len(clean_summary)}"
    )

    print(
        f"Controlled failures detected: "
        f"{detected}/"
        f"{len(scenario_summary)}"
    )

    if (
        clean_passed
        == len(clean_summary)
        and detected
        == len(scenario_summary)
    ):
        print(
            "\nVALIDATION SUITE PASSED."
        )
    else:
        print(
            "\nVALIDATION SUITE FAILED."
        )


if __name__ == "__main__":
    main()