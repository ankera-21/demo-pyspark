WITH parsed_data AS (
    SELECT
        *,
        TIMESTAMP_SUB(INFORM_TIME, 
            MAKE_INTERVAL(
                CAST(SPLIT(uptime, ' ')[0] AS INT64), 
                CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[0] AS INT64), 
                CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[1] AS INT64), 
                CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[2] AS INT64)
            )
        ) AS REBOOT_TIME
    FROM df_tbl
),
reboot_counts AS (
    SELECT
        *,
        COUNT(DISTINCT REBOOT_TIME) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ) AS REBOOT_COUNT
    FROM parsed_data
)
SELECT
    *,
    CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END AS ADJ_REBOOT_COUNT,
    MAX(CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END) OVER (
        PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
    ) AS MAX_REBOOT_COUNT
FROM reboot_counts;



WITH parsed_uptime AS (
    SELECT
        *,
        INTERVAL CAST(SPLIT(uptime, ' ')[0] AS INT64) DAY +
        INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[0] AS INT64) HOUR +
        INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[1] AS INT64) MINUTE +
        INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[2] AS INT64) SECOND AS service_uptime_interval
    FROM df_tbl
),
reboot_calc AS (
    SELECT
        *,
        TIMESTAMP_SUB(INFORM_TIME, service_uptime_interval) AS REBOOT_TIME
    FROM parsed_uptime
),
reboot_count_calc AS (
    SELECT
        *,
        COUNT(DISTINCT REBOOT_TIME) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID, REBOOT_TIME
            ORDER BY INFORM_TIME
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS REBOOT_COUNT
    FROM reboot_calc
),
final_calc AS (
    SELECT
        *,
        CASE 
            WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT 
            ELSE 0 
        END AS REBOOT_COUNT_ADJ
    FROM reboot_count_calc
)
SELECT
    *,
    MAX(REBOOT_COUNT_ADJ) OVER (
        PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ORDER BY INFORM_TIME
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS REBOOTY_COUNT
FROM final_calc;



WITH parsed_data AS (
    SELECT
        *,
        TIMESTAMP_SUB(INFORM_TIME, INTERVAL CAST(SPLIT(uptime, ' ')[0] AS INT64) DAY +
                                 INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[0] AS INT64) HOUR +
                                 INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[1] AS INT64) MINUTE +
                                 INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[2] AS INT64) SECOND) AS REBOOT_TIME
    FROM df_tbl
),
reboot_counts AS (
    SELECT
        *,
        COUNT(DISTINCT REBOOT_TIME) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ) AS REBOOT_COUNT
    FROM parsed_data
)
SELECT
    *,
    CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END AS ADJ_REBOOT_COUNT,
    MAX(CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END) OVER (
        PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
    ) AS MAX_REBOOT_COUNT
FROM reboot_counts;











-----


WITH parsed_data AS (
    SELECT
        *,
        TIMESTAMP_SUB(
            TIMESTAMP_SUB(
                TIMESTAMP_SUB(
                    TIMESTAMP_SUB(
                        INFORM_TIME, 
                        INTERVAL CAST(SPLIT(uptime, ' ')[0] AS INT64) DAY
                    ), 
                    INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[0] AS INT64) HOUR
                ), 
                INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[1] AS INT64) MINUTE
            ), 
            INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[2] AS INT64) SECOND
        ) AS REBOOT_TIME
    FROM df_tbl
),
reboot_counts AS (
    SELECT
        *,
        COUNT(DISTINCT REBOOT_TIME) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ) AS REBOOT_COUNT
    FROM parsed_data
)
SELECT
    *,
    CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END AS ADJ_REBOOT_COUNT,
    MAX(CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END) OVER (
        PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
    ) AS MAX_REBOOT_COUNT
FROM reboot_counts;

