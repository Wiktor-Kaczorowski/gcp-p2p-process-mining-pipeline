CREATE OR REPLACE TABLE
    `p2p-process-mining-pipeline.analytics.transitions`

CLUSTER BY
    from_activity,
    to_activity

AS

WITH transition_statistics AS (

    SELECT

        activity
            AS from_activity,

        next_activity
            AS to_activity,

        COUNT(*)
            AS transition_count,

        COUNT(DISTINCT case_id)
            AS case_count,

        AVG(seconds_to_next_event)
            AS average_transition_seconds,

        APPROX_QUANTILES(
            seconds_to_next_event,
            100
            IGNORE NULLS
        )[OFFSET(50)]
            AS median_transition_seconds,

        APPROX_QUANTILES(
            seconds_to_next_event,
            100
            IGNORE NULLS
        )[OFFSET(90)]
            AS p90_transition_seconds,

        MIN(seconds_to_next_event)
            AS minimum_transition_seconds,

        MAX(seconds_to_next_event)
            AS maximum_transition_seconds

    FROM
        `p2p-process-mining-pipeline.staging.events`

    WHERE
        next_activity IS NOT NULL

    GROUP BY
        from_activity,
        to_activity

)

SELECT

    *,

    SAFE_DIVIDE(
        transition_count,
        SUM(transition_count) OVER ()
    ) AS transition_share

FROM
    transition_statistics;