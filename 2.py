-- Most Earlier Longer Version
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
),
final_counts AS (
    SELECT
        *,
        CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END AS ADJ_REBOOT_COUNT,
        MAX(CASE WHEN uptime LIKE '0000 00%' THEN REBOOT_COUNT ELSE 0 END) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ) AS MAX_REBOOT_COUNT
    FROM reboot_counts
)
SELECT * FROM final_counts;


----


-- Optimized Version
WITH parsed_data AS (
    SELECT
        *,
        TIMESTAMP_SUB(
            INFORM_TIME, 
            INTERVAL CAST(SPLIT(uptime, ' ')[0] AS INT64) DAY
        ) AS temp_time
    FROM df_tbl
),
parsed_data_final AS (
    SELECT
        *,
        TIMESTAMP_SUB(
            TIMESTAMP_SUB(
                TIMESTAMP_SUB(
                    temp_time,
                    INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[0] AS INT64) HOUR
                ),
                INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[1] AS INT64) MINUTE
            ),
            INTERVAL CAST(SPLIT(SPLIT(uptime, ' ')[1], ':')[2] AS INT64) SECOND
        ) AS REBOOT_TIME
    FROM parsed_data
),
reboot_counts AS (
    SELECT
        INFORM_DATE, INFORM_HR, DEVICE_ID, uptime, REBOOT_TIME,
        COUNT(DISTINCT REBOOT_TIME) OVER (
            PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
        ) AS REBOOT_COUNT
    FROM parsed_data_final
)
SELECT
    *,
    IF(uptime LIKE '0000 00%', REBOOT_COUNT, 0) AS ADJ_REBOOT_COUNT,
    MAX(IF(uptime LIKE '0000 00%', REBOOT_COUNT, 0)) OVER (
        PARTITION BY INFORM_DATE, INFORM_HR, DEVICE_ID
    ) AS MAX_REBOOT_COUNT
FROM reboot_counts;


# REGEXP_CONTAINS(uptime, r'^\d{4} \d{2}:\d{2}:\d{2}$')
