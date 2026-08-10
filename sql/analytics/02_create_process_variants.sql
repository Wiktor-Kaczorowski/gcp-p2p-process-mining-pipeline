CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.analytics.process_variants`

AS

WITH case_variants AS (

    SELECT

        case_id,

        STRING_AGG(
            activity,
            ' -> '
            ORDER BY process_event_sequence
        ) AS process_variant

    FROM
        `p2p-process-mining-pipeline.staging.events`

    GROUP BY
        case_id

),

variant_statistics AS (

    SELECT

        process_variant,

        COUNT(*) AS case_count,

        COUNTIF(
            case_kpis.is_analysis_ready
        ) AS analysis_ready_case_count,

        AVG(
            IF(
                case_kpis.is_analysis_ready,
                case_kpis.case_duration_hours,
                NULL
            )
        ) AS average_case_duration_hours,

        APPROX_QUANTILES(
            IF(
                case_kpis.is_analysis_ready,
                case_kpis.case_duration_hours,
                NULL
            ),
            100
            IGNORE NULLS
        )[OFFSET(50)]
            AS median_case_duration_hours,

        APPROX_QUANTILES(
            IF(
                case_kpis.is_analysis_ready,
                case_kpis.case_duration_hours,
                NULL
            ),
            100
            IGNORE NULLS
        )[OFFSET(90)]
            AS p90_case_duration_hours,

        AVG(
            case_kpis.event_count
        ) AS average_event_count,

        COUNTIF(
            case_kpis.has_rework
        ) AS rework_case_count,

        SAFE_DIVIDE(
            COUNTIF(
                case_kpis.has_rework
            ),
            COUNT(*)
        ) AS rework_case_rate

    FROM
        case_variants

    INNER JOIN
        `p2p-process-mining-pipeline.analytics.case_kpis`
            AS case_kpis

    USING (case_id)

    GROUP BY
        process_variant

),

ranked_variants AS (

    SELECT

        *,

        ROW_NUMBER() OVER (
            ORDER BY
                case_count DESC,
                process_variant
        ) AS variant_rank,

        SAFE_DIVIDE(
            case_count,
            SUM(case_count) OVER ()
        ) AS case_share

    FROM
        variant_statistics

)

SELECT

    variant_rank,

    TO_HEX(
        SHA256(process_variant)
    ) AS variant_id,

    process_variant,

    case_count,
    case_share,

    analysis_ready_case_count,

    average_case_duration_hours,
    median_case_duration_hours,
    p90_case_duration_hours,

    average_event_count,

    rework_case_count,
    rework_case_rate

FROM
    ranked_variants;