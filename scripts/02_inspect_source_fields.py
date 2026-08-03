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
CASE_NAME_COLUMN = "case:Name"
ACTIVITY_COLUMN = "concept:name"
TIMESTAMP_COLUMN = "time:timestamp"

USER_COLUMN = "User"
RESOURCE_COLUMN = "org:resource"


def find_source_file() -> Path:
    """Return the available BPI Challenge 2019 source file."""

    for candidate in SOURCE_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "BPI Challenge 2019 source file not found in data/source."
    )


def load_event_data(source_path: Path) -> pd.DataFrame:
    """Load the XES event log as a pandas DataFrame."""

    print(f"Loading source file: {source_path.name}")

    event_data = pm4py.read_xes(str(source_path))

    if not isinstance(event_data, pd.DataFrame):
        event_data = pm4py.convert_to_dataframe(event_data)

    if event_data.empty:
        raise ValueError("The imported event log is empty.")

    event_data[TIMESTAMP_COLUMN] = pd.to_datetime(
        event_data[TIMESTAMP_COLUMN],
        errors="coerce",
        utc=True,
    )

    return event_data


def save_year_distribution(dataframe: pd.DataFrame) -> None:
    """Save the number of events recorded in each calendar year."""

    year_distribution = (
        dataframe[TIMESTAMP_COLUMN]
        .dt.year
        .value_counts(dropna=False)
        .rename_axis("event_year")
        .reset_index(name="event_count")
        .sort_values("event_year")
    )

    year_distribution["event_percentage"] = (
        year_distribution["event_count"]
        .div(len(dataframe))
        .mul(100)
        .round(6)
    )

    output_path = OUTPUT_DIRECTORY / "bpi_2019_year_distribution.csv"

    year_distribution.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    print(f"Created: {output_path}")


def save_earliest_events(dataframe: pd.DataFrame) -> None:
    """Save the earliest events for timestamp investigation."""

    selected_columns = [
        CASE_COLUMN,
        ACTIVITY_COLUMN,
        TIMESTAMP_COLUMN,
        USER_COLUMN,
        RESOURCE_COLUMN,
        "case:Purchasing Document",
        "case:Item",
        "case:Document Type",
        "case:Vendor",
        "case:Company",
        "case:Source",
    ]

    available_columns = [
        column
        for column in selected_columns
        if column in dataframe.columns
    ]

    earliest_events = (
        dataframe[available_columns]
        .sort_values(TIMESTAMP_COLUMN)
        .head(200)
    )

    output_path = OUTPUT_DIRECTORY / "bpi_2019_earliest_events.csv"

    earliest_events.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    print(f"Created: {output_path}")


def save_largest_cases(dataframe: pd.DataFrame) -> None:
    """Save cases containing the largest number of events."""

    largest_cases = (
        dataframe.groupby(CASE_COLUMN)
        .agg(
            event_count=(ACTIVITY_COLUMN, "size"),
            unique_activity_count=(ACTIVITY_COLUMN, "nunique"),
            first_event=(TIMESTAMP_COLUMN, "min"),
            last_event=(TIMESTAMP_COLUMN, "max"),
            purchasing_document=(
                "case:Purchasing Document",
                "first",
            ),
            item=("case:Item", "first"),
            vendor=("case:Vendor", "first"),
            company=("case:Company", "first"),
        )
        .reset_index()
    )

    largest_cases["duration_hours"] = (
        largest_cases["last_event"]
        - largest_cases["first_event"]
    ).dt.total_seconds().div(3600).round(2)

    largest_cases = largest_cases.sort_values(
        "event_count",
        ascending=False,
    ).head(200)

    output_path = OUTPUT_DIRECTORY / "bpi_2019_largest_cases.csv"

    largest_cases.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    print(f"Created: {output_path}")


def save_user_resource_comparison(
    dataframe: pd.DataFrame,
) -> None:
    """Compare values stored in User and org:resource."""

    comparison = dataframe[
        [
            USER_COLUMN,
            RESOURCE_COLUMN,
            ACTIVITY_COLUMN,
        ]
    ].copy()

    comparison["user_is_missing"] = comparison[USER_COLUMN].isna()
    comparison["resource_is_missing"] = comparison[RESOURCE_COLUMN].isna()

    comparison["normalized_user"] = (
        comparison[USER_COLUMN]
        .astype("string")
        .str.strip()
    )

    comparison["normalized_resource"] = (
        comparison[RESOURCE_COLUMN]
        .astype("string")
        .str.strip()
    )

    comparison["values_equal"] = (
        comparison["normalized_user"]
        == comparison["normalized_resource"]
    )

    summary = pd.DataFrame(
        [
            {
                "metric": "row_count",
                "value": int(len(comparison)),
            },
            {
                "metric": "missing_user_count",
                "value": int(comparison["user_is_missing"].sum()),
            },
            {
                "metric": "missing_resource_count",
                "value": int(
                    comparison["resource_is_missing"].sum()
                ),
            },
            {
                "metric": "equal_non_null_values",
                "value": int(
                    comparison["values_equal"].fillna(False).sum()
                ),
            },
            {
                "metric": "unique_user_values",
                "value": int(
                    dataframe[USER_COLUMN].nunique(dropna=True)
                ),
            },
            {
                "metric": "unique_resource_values",
                "value": int(
                    dataframe[RESOURCE_COLUMN].nunique(dropna=True)
                ),
            },
        ]
    )

    pairs = (
        dataframe.groupby(
            [USER_COLUMN, RESOURCE_COLUMN],
            dropna=False,
        )
        .size()
        .reset_index(name="event_count")
        .sort_values("event_count", ascending=False)
        .head(500)
    )

    summary_path = (
        OUTPUT_DIRECTORY
        / "bpi_2019_user_resource_summary.csv"
    )

    pairs_path = (
        OUTPUT_DIRECTORY
        / "bpi_2019_user_resource_pairs.csv"
    )

    summary.to_csv(
        summary_path,
        index=False,
        encoding="utf-8",
    )

    pairs.to_csv(
        pairs_path,
        index=False,
        encoding="utf-8",
    )

    print(f"Created: {summary_path}")
    print(f"Created: {pairs_path}")


def save_case_name_comparison(
    dataframe: pd.DataFrame,
) -> None:
    """Compare case:Name with case:concept:name."""

    comparison = (
        dataframe.groupby(CASE_COLUMN)
        .agg(
            case_name_first=(CASE_NAME_COLUMN, "first"),
            case_name_unique_count=(CASE_NAME_COLUMN, "nunique"),
            event_count=(ACTIVITY_COLUMN, "size"),
        )
        .reset_index()
    )

    comparison["case_id_equals_case_name"] = (
        comparison[CASE_COLUMN].astype("string")
        == comparison["case_name_first"].astype("string")
    )

    output_path = (
        OUTPUT_DIRECTORY
        / "bpi_2019_case_name_comparison.csv"
    )

    comparison.head(5_000).to_csv(
        output_path,
        index=False,
        encoding="utf-8",
    )

    summary = {
        "case_count": int(len(comparison)),
        "case_name_missing_count": int(
            comparison["case_name_first"].isna().sum()
        ),
        "cases_with_multiple_names": int(
            (
                comparison["case_name_unique_count"] > 1
            ).sum()
        ),
        "cases_where_id_equals_name": int(
            comparison["case_id_equals_case_name"].sum()
        ),
    }

    summary_path = (
        OUTPUT_DIRECTORY
        / "bpi_2019_case_name_summary.json"
    )

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(f"Created: {output_path}")
    print(f"Created: {summary_path}")


def json_safe_value(value: Any) -> Any:
    """Convert common pandas and NumPy values to JSON-safe values."""

    if value is None or pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value


def save_selected_field_profile(
    dataframe: pd.DataFrame,
) -> None:
    """Save top values and cardinality for business attributes."""

    selected_columns = [
        USER_COLUMN,
        RESOURCE_COLUMN,
        "Cumulative net worth (EUR)",
        "case:Spend area text",
        "case:Company",
        "case:Document Type",
        "case:Sub spend area text",
        "case:Purchasing Document",
        "case:Purch. Doc. Category name",
        "case:Vendor",
        "case:Item Type",
        "case:Item Category",
        "case:Spend classification text",
        "case:Source",
        CASE_NAME_COLUMN,
        "case:GR-Based Inv. Verif.",
        "case:Item",
        "case:Goods Receipt",
    ]

    profile: dict[str, Any] = {}

    for column in selected_columns:
        if column not in dataframe.columns:
            continue

        value_counts = (
            dataframe[column]
            .value_counts(dropna=False)
            .head(20)
        )

        top_values = []

        for value, count in value_counts.items():
            top_values.append(
                {
                    "value": json_safe_value(value),
                    "event_count": int(count),
                }
            )

        profile[column] = {
            "data_type": str(dataframe[column].dtype),
            "null_count": int(dataframe[column].isna().sum()),
            "unique_count": int(
                dataframe[column].nunique(dropna=True)
            ),
            "top_values": top_values,
        }

    output_path = (
        OUTPUT_DIRECTORY
        / "bpi_2019_selected_fields_profile.json"
    )

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            profile,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print(f"Created: {output_path}")


def print_console_summary(dataframe: pd.DataFrame) -> None:
    """Print selected investigation results."""

    earliest_rows = (
        dataframe[
            [
                CASE_COLUMN,
                ACTIVITY_COLUMN,
                TIMESTAMP_COLUMN,
            ]
        ]
        .sort_values(TIMESTAMP_COLUMN)
        .head(10)
    )

    largest_cases = (
        dataframe[CASE_COLUMN]
        .value_counts()
        .head(10)
    )

    print("\nEarliest 10 events")
    print("------------------")
    print(earliest_rows.to_string(index=False))

    print("\nLargest 10 cases")
    print("----------------")
    print(largest_cases.to_string())


def main() -> None:
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    source_path = find_source_file()
    dataframe = load_event_data(source_path)

    print(f"Imported shape: {dataframe.shape}")

    save_year_distribution(dataframe)
    save_earliest_events(dataframe)
    save_largest_cases(dataframe)
    save_user_resource_comparison(dataframe)
    save_case_name_comparison(dataframe)
    save_selected_field_profile(dataframe)

    print_console_summary(dataframe)

    print("\nSource-field investigation completed successfully.")


if __name__ == "__main__":
    main()