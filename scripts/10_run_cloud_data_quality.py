from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SQL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "quality"
    / "01_run_cloud_data_quality.sql"
)

OUTPUT_DIRECTORY = (
    PROJECT_ROOT
    / "artifacts"
    / "cloud"
)


def load_configuration() -> tuple[str, str]:
    """Load Google Cloud configuration."""

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


def execute_monitoring_sql(
    client: bigquery.Client,
    run_id: str,
    run_timestamp: datetime,
) -> None:
    """Execute the cloud data-quality rule suite."""

    if not SQL_PATH.exists():
        raise FileNotFoundError(
            f"SQL file not found: {SQL_PATH}"
        )

    sql = SQL_PATH.read_text(
        encoding="utf-8"
    )

    job_config = (
        bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_id",
                    "STRING",
                    run_id,
                ),
                bigquery.ScalarQueryParameter(
                    "run_timestamp_utc",
                    "TIMESTAMP",
                    run_timestamp,
                ),
            ]
        )
    )

    print(
        "Executing cloud data-quality rules."
    )

    query_job = client.query(
        sql,
        job_config=job_config,
    )

    query_job.result()

    print(
        "Cloud data-quality rules completed."
    )


def get_rule_results(
    client: bigquery.Client,
    project_id: str,
    run_id: str,
) -> pd.DataFrame:
    """Retrieve rule results for the current run."""

    sql = f"""
        SELECT
            run_id,
            run_timestamp_utc,
            rule_id,
            rule_name,
            severity,
            status,
            failed_rows,
            total_rows,
            failure_rate,
            message
        FROM
            `{project_id}.monitoring.data_quality_results`
        WHERE
            run_id = @run_id
        ORDER BY

            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'WARNING' THEN 2
                ELSE 3
            END,

            rule_id
    """

    job_config = (
        bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_id",
                    "STRING",
                    run_id,
                )
            ]
        )
    )

    rows = client.query(
        sql,
        job_config=job_config,
    ).result()

    records = [
        dict(row.items())
        for row in rows
    ]

    return pd.DataFrame(
        records
    )


def get_run_summary(
    client: bigquery.Client,
    project_id: str,
    run_id: str,
) -> dict:
    """Retrieve pipeline monitoring summary."""

    sql = f"""
        SELECT
            run_id,
            run_timestamp_utc,
            rules_executed,
            passed_rules,
            warning_rules,
            failed_rules,
            overall_status
        FROM
            `{project_id}.monitoring.pipeline_run_summary`
        WHERE
            run_id = @run_id
        LIMIT 1
    """

    job_config = (
        bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "run_id",
                    "STRING",
                    run_id,
                )
            ]
        )
    )

    rows = client.query(
        sql,
        job_config=job_config,
    ).result()

    row = next(
        iter(rows)
    )

    return dict(
        row.items()
    )


def serialize_value(value):
    """Convert values into JSON-compatible representations."""

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    return value


def save_reports(
    rule_results: pd.DataFrame,
    summary: dict,
) -> None:
    """Save local copies of cloud monitoring results."""

    OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    results_path = (
        OUTPUT_DIRECTORY
        / "cloud_data_quality_results.csv"
    )

    summary_path = (
        OUTPUT_DIRECTORY
        / "cloud_data_quality_summary.json"
    )

    rule_results.to_csv(
        results_path,
        index=False,
        encoding="utf-8",
    )

    json_summary = {
        key: serialize_value(value)
        for key, value
        in summary.items()
    }

    with summary_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            json_summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(
        "\nCreated monitoring reports:"
    )

    print(
        f"- {results_path}"
    )

    print(
        f"- {summary_path}"
    )


def print_results(
    rule_results: pd.DataFrame,
    summary: dict,
) -> None:
    """Print readable monitoring results."""

    print(
        "\nCloud Data Quality Results"
    )

    print(
        "--------------------------"
    )

    for _, row in rule_results.iterrows():

        print(
            f"{row['status']:<4} | "
            f"{row['severity']:<8} | "
            f"{row['rule_id']}"
        )

        print(
            f"       {row['message']}"
        )

    print(
        "\nPipeline monitoring summary"
    )

    print(
        "---------------------------"
    )

    print(
        f"Rules executed: "
        f"{summary['rules_executed']}"
    )

    print(
        f"Passed: "
        f"{summary['passed_rules']}"
    )

    print(
        f"Warnings: "
        f"{summary['warning_rules']}"
    )

    print(
        f"Failed: "
        f"{summary['failed_rules']}"
    )

    print(
        f"Overall status: "
        f"{summary['overall_status']}"
    )


def main() -> None:
    print(
        "Starting cloud data-quality monitoring."
    )

    project_id, region = (
        load_configuration()
    )

    client = bigquery.Client(
        project=project_id,
        location=region,
    )

    run_id = str(
        uuid.uuid4()
    )

    run_timestamp = datetime.now(
        timezone.utc
    )

    print(
        f"Run ID: {run_id}"
    )

    execute_monitoring_sql(
        client=client,
        run_id=run_id,
        run_timestamp=run_timestamp,
    )

    rule_results = (
        get_rule_results(
            client=client,
            project_id=project_id,
            run_id=run_id,
        )
    )

    summary = (
        get_run_summary(
            client=client,
            project_id=project_id,
            run_id=run_id,
        )
    )

    save_reports(
        rule_results=rule_results,
        summary=summary,
    )

    print_results(
        rule_results=rule_results,
        summary=summary,
    )

    if summary[
        "failed_rules"
    ] > 0:

        raise RuntimeError(
            "Cloud data-quality monitoring "
            "detected CRITICAL failures."
        )

    print(
        "\nCLOUD DATA QUALITY MONITORING "
        "COMPLETED SUCCESSFULLY."
    )


if __name__ == "__main__":
    main()