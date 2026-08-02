from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pm4py


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_DIRECTORY = PROJECT_ROOT / "data" / "source"
OUTPUT_DIRECTORY = PROJECT_ROOT / "artifacts" / "profiling"

SOURCE_CANDIDATES = [
    SOURCE_DIRECTORY / "BPI_Challenge_2019.xes.gz",
    SOURCE_DIRECTORY / "BPI_Challenge_2019.xes",
]

CASE_COLUMN = "case:concept:name"
ACTIVITY_COLUMN = "concept:name"
TIMESTAMP_COLUMN = "time:timestamp"


def find_source_file() -> Path:
    """Find the original BPI Challenge 2019 file."""

    for candidate in SOURCE_CANDIDATES:
        if candidate.exists():
            return candidate

    expected_paths = "\n".join(
        f"- {path}" for path in SOURCE_CANDIDATES
    )

    raise FileNotFoundError(
        "The BPI Challenge 2019 source file was not found.\n"
        "Expected one of these files:\n"
        f"{expected_paths}"
    )


def load_event_data(source_path: Path) -> pd.DataFrame:
    """Read an XES event log into a pandas DataFrame."""

    print("Loading the XES event log.")
    print(f"Source: {source_path}")
    print("This operation processes the complete event log.")

    event_data = pm4py.read_xes(str(source_path))

    # Current PM4Py versions return a DataFrame by default.
    # This fallback also supports versions returning EventLog.
    if not isinstance(event_data, pd.DataFrame):
        event_data = pm4py.convert_to_dataframe(event_data)

    if event_data.empty:
        raise ValueError("The imported event log is empty.")

    return event_data


def validate_required_columns(dataframe: pd.DataFrame) -> None:
    """Validate the minimum columns required for process mining."""

    required_columns = {
        CASE_COLUMN,
        ACTIVITY_COLUMN,
        TIMESTAMP_COLUMN,
    }

    missing_columns = required_columns.difference(dataframe.columns)

    if missing_columns:
        raise ValueError(
            "The event log does not contain all required columns. "
            f"Missing columns: {sorted(missing_columns)}"
        )


def prepare_timestamps(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert the event timestamp column into UTC timestamps."""

    result = dataframe.copy()

    result[TIMESTAMP_COLUMN] = pd.to_datetime(
        result[TIMESTAMP_COLUMN],
        errors="coerce",
        utc=True,
    )

    return result


def timestamp_to_string(value: Any) -> str | None:
    """Convert a timestamp into a JSON-safe string."""

    if value is None or pd.isna(value):
        return None

    return value.isoformat()


def build_column_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Create a report describing every source column."""

    row_count = len(dataframe)

    report_rows: list[dict[str, Any]] = []

    for column in dataframe.columns:
        null_count = int(dataframe[column].isna().sum())

        null_percentage = (
            round((null_count / row_count) * 100, 4)
            if row_count > 0
            else 0.0
        )

        unique_count = int(
            dataframe[column].nunique(dropna=True)
        )

        report_rows.append(
            {
                "column_name": column,
                "data_type": str(dataframe[column].dtype),
                "null_count": null_count,
                "null_percentage": null_percentage,
                "unique_value_count": unique_count,
            }
        )

    return pd.DataFrame(report_rows)


def build_activity_report(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Count how often every activity occurs."""

    report = (
        dataframe[ACTIVITY_COLUMN]
        .fillna("<MISSING_ACTIVITY>")
        .value_counts(dropna=False)
        .rename_axis("activity")
        .reset_index(name="event_count")
    )

    report["event_percentage"] = (
        report["event_count"]
        .div(len(dataframe))
        .mul(100)
        .round(4)
    )

    return report


def build_case_report(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Calculate basic statistics for individual process cases."""

    valid_cases = dataframe.dropna(subset=[CASE_COLUMN]).copy()

    report = (
        valid_cases.groupby(CASE_COLUMN)
        .agg(
            event_count=(ACTIVITY_COLUMN, "size"),
            first_event=(TIMESTAMP_COLUMN, "min"),
            last_event=(TIMESTAMP_COLUMN, "max"),
            unique_activity_count=(ACTIVITY_COLUMN, "nunique"),
        )
        .reset_index()
    )

    report["case_duration_hours"] = (
        report["last_event"] - report["first_event"]
    ).dt.total_seconds().div(3600).round(2)

    return report


def build_profile(
    dataframe: pd.DataFrame,
    case_report: pd.DataFrame,
    source_path: Path,
) -> dict[str, Any]:
    """Build a JSON-compatible summary of the complete event log."""

    timestamp_values = dataframe[TIMESTAMP_COLUMN]

    case_event_counts = case_report["event_count"]
    case_durations = case_report["case_duration_hours"]

    profile: dict[str, Any] = {
        "source_file": source_path.name,
        "event_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "case_count": int(
            dataframe[CASE_COLUMN].nunique(dropna=True)
        ),
        "activity_count": int(
            dataframe[ACTIVITY_COLUMN].nunique(dropna=True)
        ),
        "period_start": timestamp_to_string(
            timestamp_values.min()
        ),
        "period_end": timestamp_to_string(
            timestamp_values.max()
        ),
        "missing_case_id_count": int(
            dataframe[CASE_COLUMN].isna().sum()
        ),
        "missing_activity_count": int(
            dataframe[ACTIVITY_COLUMN].isna().sum()
        ),
        "invalid_timestamp_count": int(
            timestamp_values.isna().sum()
        ),
        "events_per_case": {
            "minimum": int(case_event_counts.min()),
            "median": float(case_event_counts.median()),
            "mean": round(float(case_event_counts.mean()), 4),
            "maximum": int(case_event_counts.max()),
        },
        "case_duration_hours": {
            "minimum": round(float(case_durations.min()), 4),
            "median": round(float(case_durations.median()), 4),
            "mean": round(float(case_durations.mean()), 4),
            "maximum": round(float(case_durations.max()), 4),
        },
        "source_columns": list(dataframe.columns),
    }

    return profile


def save_outputs(
    dataframe: pd.DataFrame,
    column_report: pd.DataFrame,
    activity_report: pd.DataFrame,
    case_report: pd.DataFrame,
    profile: dict[str, Any],
) -> None:
    """Save profiling outputs."""

    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    profile_path = (
        OUTPUT_DIRECTORY / "bpi_2019_profile.json"
    )
    columns_path = (
        OUTPUT_DIRECTORY / "bpi_2019_columns.csv"
    )
    activities_path = (
        OUTPUT_DIRECTORY / "bpi_2019_activities.csv"
    )
    cases_path = (
        OUTPUT_DIRECTORY / "bpi_2019_case_sample.csv"
    )
    sample_path = (
        OUTPUT_DIRECTORY / "bpi_2019_event_sample.parquet"
    )

    with profile_path.open("w", encoding="utf-8") as file:
        json.dump(
            profile,
            file,
            indent=2,
            ensure_ascii=False,
        )

    column_report.to_csv(
        columns_path,
        index=False,
        encoding="utf-8",
    )

    activity_report.to_csv(
        activities_path,
        index=False,
        encoding="utf-8",
    )

    case_report.head(1_000).to_csv(
        cases_path,
        index=False,
        encoding="utf-8",
    )

    dataframe.head(5_000).to_parquet(
        sample_path,
        index=False,
    )

    print("\nCreated files:")
    print(f"- {profile_path}")
    print(f"- {columns_path}")
    print(f"- {activities_path}")
    print(f"- {cases_path}")
    print(f"- {sample_path}")


def print_summary(profile: dict[str, Any]) -> None:
    """Print the most important profiling results."""

    events_per_case = profile["events_per_case"]

    print("\nBPI Challenge 2019 profiling summary")
    print("------------------------------------")
    print(f"Source file: {profile['source_file']}")
    print(f"Events: {profile['event_count']:,}")
    print(f"Cases: {profile['case_count']:,}")
    print(f"Activities: {profile['activity_count']:,}")
    print(f"Columns: {profile['column_count']:,}")
    print(f"Period start: {profile['period_start']}")
    print(f"Period end: {profile['period_end']}")
    print(
        "Missing case identifiers: "
        f"{profile['missing_case_id_count']:,}"
    )
    print(
        "Missing activities: "
        f"{profile['missing_activity_count']:,}"
    )
    print(
        "Invalid timestamps: "
        f"{profile['invalid_timestamp_count']:,}"
    )
    print(
        "Events per case — "
        f"min: {events_per_case['minimum']}, "
        f"median: {events_per_case['median']}, "
        f"mean: {events_per_case['mean']}, "
        f"max: {events_per_case['maximum']}"
    )


def main() -> None:
    source_path = find_source_file()

    dataframe = load_event_data(source_path)

    print(f"Imported shape: {dataframe.shape}")

    validate_required_columns(dataframe)

    dataframe = prepare_timestamps(dataframe)

    print("Building column report.")
    column_report = build_column_report(dataframe)

    print("Building activity report.")
    activity_report = build_activity_report(dataframe)

    print("Building case report.")
    case_report = build_case_report(dataframe)

    print("Building general profile.")
    profile = build_profile(
        dataframe=dataframe,
        case_report=case_report,
        source_path=source_path,
    )

    save_outputs(
        dataframe=dataframe,
        column_report=column_report,
        activity_report=activity_report,
        case_report=case_report,
        profile=profile,
    )

    print_summary(profile)

    print("\nProfiling completed successfully.")


if __name__ == "__main__":
    main()