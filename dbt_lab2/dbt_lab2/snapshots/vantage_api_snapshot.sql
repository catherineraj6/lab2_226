{% snapshot stock_price_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='symbol_date',
        strategy='timestamp',
        updated_at='symbol_date'
    )
}}

    SELECT
        date AS symbol_date,
        symbol,
        open,
        high,
        low,
        close,
        volume
    FROM {{ source('raw_data', 'vantage_api') }}

{% endsnapshot %}