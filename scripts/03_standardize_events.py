from __future__ import annotations

import gc
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pm4py


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_DIRECTORY = PROJECT_ROOT / "data" / "source"
PROCESSED_DIRECTORY = PROJECT_ROOT / "data" / "processed"
PARTITIONED_OUTPUT_DIRECTORY = (
    PROCESSED_DIRECTORY / "canonical_events"
)
PROFILING_DIRECTORY = PROJECT_ROOT / "artifacts" / "profiling"

SOURCE_CANDIDATES = [
    SOURCE_DIRECTORY / "BPI_Challenge_2019.xes.gz",
    SOURCE_DIRECTORY / "BPI_Challenge_2019.xes",
]

SOURCE_TO_CANONICAL = {
    "concept:name": "activity",
    "time:timestamp": "event_timestamp",
    "org:resource": "resource_id",
    "Cumulative net worth (EUR)": "cumulative_net_worth_eur",
    "case:Spend area text": "spend_area",
    "case:Company": "company_id",
    "case:Document Type": "document_type",
    "case:Sub spend area text": "sub_spend_area",
    "case:Purchasing Document": "purchasing_document_id",
    "case:Purch. Doc. Category name": (
        "purchasing_document_category"
    ),
    "case:Vendor": "vendor_id",
    "case:Item Type": "item_type",
    "case:Item Category": "item_category",
    "case:Spend classification text": "spend_classification",
    "case:Source": "source_system",
    "case:Name": "case_name",
    "case:GR-Based Inv. Verif.": (
        "gr_based_invoice_verification"
    ),
    "case:Item": "item_id",
    "case:concept:name": "case_id",
    "case:Goods Receipt": "goods_receipt_required",
}

CANONICAL_SOURCE_COLUMNS = list(SOURCE_TO_CANONICAL.values())

IDENTIFIER_COLUMNS = [
    "case_id",
    "resource_id",
    "company_id",
    "purchasing_document_id",
    "vendor_id",
    "source_system",
    "item_id",
]

TEXT_COLUMNS = [
    "activity",
    "spend_area",
    "document_type",
    "sub_spend_area",
    "purchasing_document_category",
    "item_type",
    "item_category",
    "spend_classification",
    "case_name",
    "gr_based_invoice_verification",
    "goods_receipt_required",
]

DUPLICATE_CHECK_COLUMNS = [
    "case_id",
    "activity",
    "event_timestamp",
    "resource_id",
    "purchasing_document_id",
    "item_id",
]


def find_source_file() -> Path:
    """Return the available BPI Challenge 2019 source file."""

    for candidate in SOURCE_CANDIDATES:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "BPI Challenge 2019 source file not found in data/source."
    )


def load_event_data(source_path: Path) -> pd.DataFrame:
    """Load the original XES event log."""

    print(f"Loading source file: {source_path.name}")

    dataframe = pm4py.read_xes(str(source_path))

    if not isinstance(dataframe, pd.DataFrame):
        dataframe = pm4py.convert_to_dataframe(dataframe)

    if dataframe.empty:
        raise ValueError("The imported event log is empty.")

    print(f"Imported shape: {dataframe.shape}")

    return dataframe


def validate_source_schema(dataframe: pd.DataFrame) -> None:
    """Check whether every expected source column is available."""

    expected_columns = set(SOURCE_TO_CANONICAL)
    missing_columns = expected_columns.difference(dataframe.columns)

    if missing_columns:
        raise ValueError(
            "Expected source columns are missing: "
            f"{sorted(missing_columns)}"
        )


def normalize_string_column(series: pd.Series) -> pd.Series:
    """Convert a source column into a clean nullable string."""

    result = series.astype("string").str.strip()
    result = result.replace("", pd.NA)

    return result


def classify_timestamp_quality(
    event_timestamps: pd.Series,
) -> pd.Series:
    """Classify timestamps without deleting or changing them."""

    years = event_timestamps.dt.year

    status = pd.Series(
        "expected_period",
        index=event_timestamps.index,
        dtype="string",
    )

    status.loc[years < 2017] = "historical_outlier"

    status.loc[years == 2017] = (
        "edge_period_before_main_window"
    )

    status.loc[years == 2020] = (
        "edge_period_after_main_window"
    )

    status.loc[event_timestamps.isna()] = "invalid_timestamp"

    return status


def build_canonical_events(
    source_dataframe: pd.DataFrame,
    source_path: Path,
) -> pd.DataFrame:
    """Create the standardized canonical event table."""

    validate_source_schema(source_dataframe)

    source_dataframe.insert(
        0,
        "source_row_number",
        range(1, len(source_dataframe) + 1),
    )

    source_dataframe.rename(
        columns=SOURCE_TO_CANONICAL,
        inplace=True,
    )

    selected_columns = [
        "source_row_number",
        *CANONICAL_SOURCE_COLUMNS,
    ]

    canonical = source_dataframe[selected_columns].copy()

    del source_dataframe
    gc.collect()

    print("Normalizing identifiers.")

    for column in IDENTIFIER_COLUMNS:
        canonical[column] = normalize_string_column(
            canonical[column]
        )

    # In the original source, NONE represents a missing resource.
    canonical["resource_id"] = canonical["resource_id"].replace(
        {
            "NONE": pd.NA,
            "None": pd.NA,
            "none": pd.NA,
        }
    )

    print("Normalizing descriptive fields.")

    for column in TEXT_COLUMNS:
        canonical[column] = normalize_string_column(
            canonical[column]
        )

    print("Converting timestamps and numerical values.")

    canonical["event_timestamp"] = pd.to_datetime(
        canonical["event_timestamp"],
        errors="coerce",
        utc=True,
    )

    canonical["cumulative_net_worth_eur"] = pd.to_numeric(
        canonical["cumulative_net_worth_eur"],
        errors="coerce",
    )

    canonical["event_year"] = (
        canonical["event_timestamp"]
        .dt.year
        .astype("Int16")
    )

    canonical["event_month"] = (
        canonical["event_timestamp"]
        .dt.month
        .astype("Int8")
    )

    canonical["event_date"] = (
        canonical["event_timestamp"]
        .dt.date
    )

    canonical["timestamp_quality_status"] = (
        classify_timestamp_quality(
            canonical["event_timestamp"]
        )
    )

    canonical["source_file"] = source_path.name

    canonical["pipeline_batch_id"] = (
        "bpi2019_full_snapshot_v1"
    )

    processing_timestamp = datetime.now(timezone.utc)

    canonical["processing_timestamp_utc"] = (
        processing_timestamp
    )

    print("Sorting events inside cases.")

    canonical.sort_values(
        by=[
            "case_id",
            "event_timestamp",
            "source_row_number",
        ],
        kind="mergesort",
        inplace=True,
    )

    canonical["event_sequence"] = (
        canonical.groupby(
            "case_id",
            sort=False,
        )
        .cumcount()
        .add(1)
        .astype("int32")
    )

    # This identifier remains stable as long as the source XES order
    # remains unchanged.
    canonical["event_id"] = canonical[
        "source_row_number"
    ].astype("int64")

    final_column_order = [
        "event_id",
        "case_id",
        "case_name",
        "event_sequence",
        "activity",
        "event_timestamp",
        "event_date",
        "event_year",
        "event_month",
        "timestamp_quality_status",
        "resource_id",
        "cumulative_net_worth_eur",
        "purchasing_document_id",
        "item_id",
        "document_type",
        "purchasing_document_category",
        "company_id",
        "vendor_id",
        "source_system",
        "spend_area",
        "sub_spend_area",
        "spend_classification",
        "item_type",
        "item_category",
        "gr_based_invoice_verification",
        "goods_receipt_required",
        "source_file",
        "source_row_number",
        "pipeline_batch_id",
        "processing_timestamp_utc",
    ]

    return canonical[final_column_order]


def prepare_output_directory() -> None:
    """Clear the previous partitioned output."""

    if PARTITIONED_OUTPUT_DIRECTORY.exists():
        shutil.rmtree(PARTITIONED_OUTPUT_DIRECTORY)

    PARTITIONED_OUTPUT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    PROFILING_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )


def write_partitioned_parquet(
    canonical: pd.DataFrame,
) -> int:
    """Write one Parquet file for each year and month."""

    partition_count = 0

    grouped = canonical.groupby(
        ["event_year", "event_month"],
        dropna=False,
        sort=True,
    )

    for (event_year, event_month), partition in grouped:
        if pd.isna(event_year) or pd.isna(event_month):
            year_directory = "event_year=unknown"
            month_directory = "event_month=unknown"
        else:
            year_directory = (
                f"event_year={int(event_year):04d}"
            )
            month_directory = (
                f"event_month={int(event_month):02d}"
            )

        output_directory = (
            PARTITIONED_OUTPUT_DIRECTORY
            / year_directory
            / month_directory
        )

        output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = output_directory / "events.parquet"

        partition_to_write = partition.drop(
            columns=[
        "event_year",
        "event_month",
    ]
)
        partition_to_write.to_parquet(
    output_path,
    index=False,
    compression="snappy",
)
        print(
    f"Written {len(partition_to_write):,} rows to "
    f"{output_path.relative_to(PROJECT_ROOT)}"
)
        partition_count += 1

    return partition_count


def build_standardization_summary(
    canonical: pd.DataFrame,
    partition_count: int,
) -> dict[str, Any]:
    """Create a summary of the standardized dataset."""

    timestamp_quality_counts = (
        canonical["timestamp_quality_status"]
        .value_counts(dropna=False)
        .to_dict()
    )

    potential_duplicate_mask = canonical.duplicated(
        subset=DUPLICATE_CHECK_COLUMNS,
        keep=False,
    )

    summary: dict[str, Any] = {
        "row_count": int(len(canonical)),
        "case_count": int(
            canonical["case_id"].nunique(dropna=True)
        ),
        "activity_count": int(
            canonical["activity"].nunique(dropna=True)
        ),
        "column_count": int(len(canonical.columns)),
        "partition_count": int(partition_count),
        "resource_missing_count_after_normalization": int(
            canonical["resource_id"].isna().sum()
        ),
        "case_id_missing_count": int(
            canonical["case_id"].isna().sum()
        ),
        "activity_missing_count": int(
            canonical["activity"].isna().sum()
        ),
        "timestamp_missing_count": int(
            canonical["event_timestamp"].isna().sum()
        ),
        "potential_duplicate_event_row_count": int(
            potential_duplicate_mask.sum()
        ),
        "timestamp_quality_counts": {
            str(key): int(value)
            for key, value in timestamp_quality_counts.items()
        },
        "period_start": (
            canonical["event_timestamp"].min().isoformat()
        ),
        "period_end": (
            canonical["event_timestamp"].max().isoformat()
        ),
        "dropped_redundant_source_columns": [
            "User",
        ],
    }

    return summary


def save_profiling_outputs(
    canonical: pd.DataFrame,
    summary: dict[str, Any],
) -> None:
    """Save schema, sample, timestamp, and summary reports."""

    summary_path = (
        PROFILING_DIRECTORY
        / "bpi_2019_standardization_summary.json"
    )

    schema_path = (
        PROFILING_DIRECTORY
        / "bpi_2019_canonical_schema.csv"
    )

    sample_path = (
        PROFILING_DIRECTORY
        / "bpi_2019_canonical_sample.csv"
    )

    timestamp_quality_path = (
        PROFILING_DIRECTORY
        / "bpi_2019_timestamp_quality.csv"
    )

    timestamp_outlier_activities_path = (
        PROFILING_DIRECTORY
        / "bpi_2019_timestamp_outlier_activities.csv"
    )

    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
        )

    schema_report = pd.DataFrame(
        {
            "column_name": canonical.columns,
            "data_type": [
                str(canonical[column].dtype)
                for column in canonical.columns
            ],
            "null_count": [
                int(canonical[column].isna().sum())
                for column in canonical.columns
            ],
            "unique_value_count": [
                int(canonical[column].nunique(dropna=True))
                for column in canonical.columns
            ],
        }
    )

    schema_report.to_csv(
        schema_path,
        index=False,
        encoding="utf-8",
    )

    canonical.head(1_000).to_csv(
        sample_path,
        index=False,
        encoding="utf-8",
    )

    timestamp_quality_report = (
        canonical["timestamp_quality_status"]
        .value_counts(dropna=False)
        .rename_axis("timestamp_quality_status")
        .reset_index(name="event_count")
    )

    timestamp_quality_report["event_percentage"] = (
        timestamp_quality_report["event_count"]
        .div(len(canonical))
        .mul(100)
        .round(6)
    )

    timestamp_quality_report.to_csv(
        timestamp_quality_path,
        index=False,
        encoding="utf-8",
    )

    timestamp_outlier_activities = (
        canonical.loc[
            canonical["timestamp_quality_status"]
            == "historical_outlier"
        ]
        .groupby(
            [
                "event_year",
                "activity",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="event_count")
        .sort_values(
            [
                "event_year",
                "event_count",
            ],
            ascending=[
                True,
                False,
            ],
        )
    )

    timestamp_outlier_activities.to_csv(
        timestamp_outlier_activities_path,
        index=False,
        encoding="utf-8",
    )

    print("\nCreated profiling outputs:")
    print(f"- {summary_path}")
    print(f"- {schema_path}")
    print(f"- {sample_path}")
    print(f"- {timestamp_quality_path}")
    print(f"- {timestamp_outlier_activities_path}")


def print_summary(summary: dict[str, Any]) -> None:
    """Print the most important standardization results."""

    print("\nCanonical event table summary")
    print("-----------------------------")
    print(f"Rows: {summary['row_count']:,}")
    print(f"Cases: {summary['case_count']:,}")
    print(f"Activities: {summary['activity_count']:,}")
    print(f"Columns: {summary['column_count']:,}")
    print(f"Partitions: {summary['partition_count']:,}")
    print(
        "Missing resources after NONE normalization: "
        f"{summary['resource_missing_count_after_normalization']:,}"
    )
    print(
        "Potential duplicate event rows: "
        f"{summary['potential_duplicate_event_row_count']:,}"
    )
    print("Timestamp quality:")

    for status, count in summary[
        "timestamp_quality_counts"
    ].items():
        print(f"- {status}: {count:,}")


def main() -> None:
    source_path = find_source_file()

    source_dataframe = load_event_data(source_path)

    canonical = build_canonical_events(
        source_dataframe=source_dataframe,
        source_path=source_path,
    )

    prepare_output_directory()

    partition_count = write_partitioned_parquet(canonical)

    summary = build_standardization_summary(
        canonical=canonical,
        partition_count=partition_count,
    )

    save_profiling_outputs(
        canonical=canonical,
        summary=summary,
    )

    print_summary(summary)

    print("\nEvent standardization completed successfully.")


if __name__ == "__main__":
    main()