# Process Analytics Model

## Objective

The BigQuery ANALYTICS layer converts the process-oriented
STAGING event log into reusable process-mining metrics.

## Tables

### analytics.case_kpis

One row per process case.

Provides:

- case cycle time
- event and activity counts
- repeated-activity / rework indicators
- resource completeness
- timestamp-quality indicators
- source batch traceability
- organizational and purchasing attributes

### analytics.process_variants

One row per unique activity sequence.

Provides:

- variant ranking
- process sequence
- case frequency
- case share
- median and P90 cycle time
- rework rate
- analysis-ready case count

### analytics.activity_performance

One row per activity.

Provides:

- event frequency
- case frequency
- resource completeness
- start and end frequency
- median inter-event time
- P90 inter-event time

### analytics.transitions

One row per directly-follows relationship:

`from_activity -> to_activity`

Provides:

- transition frequency
- affected process cases
- average duration
- median duration
- P90 duration
- global transition share

The table supports process-flow and bottleneck analysis.

### analytics.process_overview

Single-row reporting table containing high-level process KPIs.

## Data-quality-aware analytics

Known timestamp-quality anomalies remain available in the
warehouse.

Cases containing timestamp outliers are identified using
`is_analysis_ready`.

Cycle-time metrics in the overview and process-variant tables are
calculated using analysis-ready cases so that preserved historical
source anomalies do not silently distort performance metrics.

## Process-mining concepts

The analytical model supports:

- process variants
- directly-follows relationships
- cycle time
- rework indicators
- bottleneck analysis
- case-level performance
- activity-level performance