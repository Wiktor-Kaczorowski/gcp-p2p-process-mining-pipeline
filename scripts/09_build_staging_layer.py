from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SQL_DIRECTORY = (
    PROJECT_ROOT
    / "sql"
    / "staging"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "cloud"
)


SQL_FILES = [
    "01_create_staging_events.sql",
    "02_create_staging_cases.sql",
]


def load_configuration() -> tuple[str, str]:
    """Load BigQuery configuration."""

    load_dotenv(
        PROJECT_ROOT / ".env"
    )

    project_id = os.getenv(
        "GCP_PROJECT_ID"
    )

    region = os.getenv(
        "GCP_REGION"
    )

    if not project_id:
        raise ValueError(
            "GCP_PROJECT_ID is missing."
        )

    if not region:
        raise ValueError(
            "GCP_REGION is missing."
        )

    return project_id, region


def execute_sql_file(
    client: bigquery.Client,
    sql_file_name: str,
) -> None:
    """Execute one SQL transformation file."""

    sql_path = (
        SQL_DIRECTORY
        / sql_file_name
    )

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    sql = sql_path.read_text(
        encoding="utf-8"
    )

    print(
        f"Executing: {sql_file_name}"
    )

    query_job = client.query(
        sql
    )

    query_job.result()

    print(
        f"Completed: {sql_file_name}"
    )


def collect_metrics(
    client: bigquery.Client,
    project_id: str,
) -> dict:
    """Collect reconciliation metrics."""

    sql = f"""
        SELECT

            (
                SELECT COUNT(*)
                FROM `{project_id}.raw.events`
            ) AS raw_rows,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.events`
            ) AS staging_rows,

            (
                SELECT COUNT(DISTINCT event_id)
                FROM `{project_id}.staging.events`
            ) AS unique_event_ids,

            (
                SELECT COUNT(DISTINCT case_id)
                FROM `{project_id}.staging.events`
            ) AS event_case_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.cases`
            ) AS case_table_rows,

            (
                SELECT COUNT(DISTINCT activity)
                FROM `{project_id}.staging.events`
            ) AS activity_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.events`
                WHERE case_id IS NULL
            ) AS missing_case_ids,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.events`
                WHERE activity IS NULL
            ) AS missing_activities,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.events`
                WHERE event_timestamp IS NULL
            ) AS missing_timestamps

    """

    result = next(
        iter(
            client.query(
                sql
            ).result()
        )
    )

    return {
        key: int(result[key])
        for key in result.keys()
    }


def save_summary(
    metrics: dict,
    project_id: str,
) -> None:
    """Save local staging build report."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary = {
        "project_id": project_id,
        "dataset": "staging",
        "build_timestamp_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        **metrics,
        "row_reconciliation_success": (
            metrics["raw_rows"]
            == metrics["staging_rows"]
            == metrics["unique_event_ids"]
        ),
        "case_reconciliation_success": (
            metrics["event_case_count"]
            == metrics["case_table_rows"]
        ),
        "mandatory_fields_success": (
            metrics["missing_case_ids"] == 0
            and metrics["missing_activities"] == 0
            and metrics["missing_timestamps"] == 0
        ),
    }

    summary["staging_build_success"] = (
        summary[
            "row_reconciliation_success"
        ]
        and summary[
            "case_reconciliation_success"
        ]
        and summary[
            "mandatory_fields_success"
        ]
    )

    output_path = (
        OUTPUT_DIRECTORY
        / "bigquery_staging_summary.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
        )

    print(
        f"\nCreated staging report: "
        f"{output_path}"
    )


def main() -> None:
    print(
        "Starting BigQuery STAGING build."
    )

    project_id, region = (
        load_configuration()
    )

    client = bigquery.Client(
        project=project_id,
        location=region,
    )

    for sql_file in SQL_FILES:
        execute_sql_file(
            client=client,
            sql_file_name=sql_file,
        )

    metrics = collect_metrics(
        client=client,
        project_id=project_id,
    )

    save_summary(
        metrics=metrics,
        project_id=project_id,
    )

    print(
        "\nBigQuery STAGING summary"
    )

    print(
        "------------------------"
    )

    print(
        f"RAW rows: "
        f"{metrics['raw_rows']:,}"
    )

    print(
        f"STAGING rows: "
        f"{metrics['staging_rows']:,}"
    )

    print(
        f"Unique event IDs: "
        f"{metrics['unique_event_ids']:,}"
    )

    print(
        f"Cases in events: "
        f"{metrics['event_case_count']:,}"
    )

    print(
        f"Cases table rows: "
        f"{metrics['case_table_rows']:,}"
    )

    print(
        f"Activities: "
        f"{metrics['activity_count']:,}"
    )

    print(
        f"Missing case IDs: "
        f"{metrics['missing_case_ids']:,}"
    )

    print(
        f"Missing activities: "
        f"{metrics['missing_activities']:,}"
    )

    print(
        f"Missing timestamps: "
        f"{metrics['missing_timestamps']:,}"
    )

    success = (
        metrics["raw_rows"]
        == metrics["staging_rows"]
        == metrics[
            "unique_event_ids"
        ]
        and metrics[
            "event_case_count"
        ]
        == metrics[
            "case_table_rows"
        ]
        and metrics[
            "missing_case_ids"
        ]
        == 0
        and metrics[
            "missing_activities"
        ]
        == 0
        and metrics[
            "missing_timestamps"
        ]
        == 0
    )

    if success:
        print(
            "\nBIGQUERY STAGING BUILD "
            "COMPLETED SUCCESSFULLY."
        )

    else:
        raise RuntimeError(
            "STAGING reconciliation failed."
        )


if __name__ == "__main__":
    main()