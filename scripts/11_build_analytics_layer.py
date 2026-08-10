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
    / "analytics"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "cloud"
)


SQL_FILES = [
    "01_create_case_kpis.sql",
    "02_create_process_variants.sql",
    "03_create_activity_performance.sql",
    "04_create_transitions.sql",
    "05_create_process_overview.sql",
]


def load_configuration() -> tuple[str, str]:
    """Load BigQuery project configuration."""

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
    file_name: str,
) -> None:
    """Execute one analytics SQL file."""

    file_path = (
        SQL_DIRECTORY
        / file_name
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"SQL file missing: {file_path}"
        )

    print(
        f"Executing: {file_name}"
    )

    sql = file_path.read_text(
        encoding="utf-8"
    )

    job = client.query(
        sql
    )

    job.result()

    print(
        f"Completed: {file_name}"
    )


def collect_metrics(
    client: bigquery.Client,
    project_id: str,
) -> dict:
    """Collect analytics reconciliation metrics."""

    sql = f"""
        SELECT

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.cases`
            ) AS staging_case_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.analytics.case_kpis`
            ) AS case_kpi_rows,

            (
                SELECT COUNT(DISTINCT case_id)
                FROM `{project_id}.analytics.case_kpis`
            ) AS unique_cases,

            (
                SELECT COUNT(*)
                FROM `{project_id}.analytics.process_variants`
            ) AS variant_count,

            (
                SELECT SUM(case_count)
                FROM `{project_id}.analytics.process_variants`
            ) AS variant_case_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.analytics.activity_performance`
            ) AS activity_count,

            (
                SELECT COUNT(DISTINCT activity)
                FROM `{project_id}.staging.events`
            ) AS expected_activity_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.analytics.transitions`
            ) AS transition_type_count,

            (
                SELECT SUM(transition_count)
                FROM `{project_id}.analytics.transitions`
            ) AS transition_count,

            (
                SELECT COUNT(*)
                FROM `{project_id}.staging.events`
                WHERE next_activity IS NOT NULL
            ) AS expected_transition_count
    """

    result = next(
        iter(
            client.query(sql).result()
        )
    )

    return {
        key: int(result[key])
        for key in result.keys()
    }


def save_summary(
    metrics: dict,
    project_id: str,
) -> dict:
    """Save analytics build summary."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    case_success = (
        metrics["staging_case_count"]
        == metrics["case_kpi_rows"]
        == metrics["unique_cases"]
        == metrics["variant_case_count"]
    )

    activity_success = (
        metrics["activity_count"]
        == metrics["expected_activity_count"]
    )

    transition_success = (
        metrics["transition_count"]
        == metrics["expected_transition_count"]
    )

    summary = {
        "project_id": project_id,
        "dataset": "analytics",
        "build_timestamp_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        **metrics,
        "case_reconciliation_success": (
            case_success
        ),
        "activity_reconciliation_success": (
            activity_success
        ),
        "transition_reconciliation_success": (
            transition_success
        ),
        "analytics_build_success": (
            case_success
            and activity_success
            and transition_success
        ),
    }

    output_path = (
        OUTPUT_DIRECTORY
        / "bigquery_analytics_summary.json"
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
        f"\nCreated analytics report: "
        f"{output_path}"
    )

    return summary


def main() -> None:

    print(
        "Starting BigQuery ANALYTICS build."
    )

    project_id, region = (
        load_configuration()
    )

    client = bigquery.Client(
        project=project_id,
        location=region,
    )

    for file_name in SQL_FILES:

        execute_sql_file(
            client=client,
            file_name=file_name,
        )

    metrics = collect_metrics(
        client=client,
        project_id=project_id,
    )

    summary = save_summary(
        metrics=metrics,
        project_id=project_id,
    )

    print(
        "\nBigQuery ANALYTICS summary"
    )

    print(
        "--------------------------"
    )

    print(
        f"Staging cases: "
        f"{metrics['staging_case_count']:,}"
    )

    print(
        f"Case KPI rows: "
        f"{metrics['case_kpi_rows']:,}"
    )

    print(
        f"Unique cases: "
        f"{metrics['unique_cases']:,}"
    )

    print(
        f"Process variants: "
        f"{metrics['variant_count']:,}"
    )

    print(
        f"Cases represented by variants: "
        f"{metrics['variant_case_count']:,}"
    )

    print(
        f"Activities: "
        f"{metrics['activity_count']:,}"
    )

    print(
        f"Transition types: "
        f"{metrics['transition_type_count']:,}"
    )

    print(
        f"Transitions: "
        f"{metrics['transition_count']:,}"
    )

    print(
        f"Expected transitions: "
        f"{metrics['expected_transition_count']:,}"
    )

    if not summary[
        "analytics_build_success"
    ]:

        raise RuntimeError(
            "ANALYTICS reconciliation failed."
        )

    print(
        "\nBIGQUERY ANALYTICS BUILD "
        "COMPLETED SUCCESSFULLY."
    )


if __name__ == "__main__":
    main()