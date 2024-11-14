{{ config(materialized='table', schema='RAW_DATA_ANALYTICS') }}

WITH price_changes AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS change
    FROM {{ source('raw_data', 'vantage_api') }}
),

average_gains_losses AS (
    SELECT
        date,
        symbol,
        close,
        change,
        -- Calculate average gain over the 14-day window
        AVG(CASE WHEN change > 0 THEN change ELSE 0 END) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        -- Calculate average loss over the 14-day window
        AVG(CASE WHEN change < 0 THEN -change ELSE 0 END) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM price_changes
)

SELECT
    date,
    symbol,
    close,
    avg_gain,
    avg_loss,
    CASE
        WHEN avg_loss = 0 THEN 100
        ELSE ROUND(100 - (100 / (1 + (avg_gain / avg_loss))), 2)
    END AS rsi_14d
FROM average_gains_losses
ORDER BY symbol, date
