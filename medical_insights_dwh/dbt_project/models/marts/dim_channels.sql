-- models/marts/dim_channels.sql

WITH source_channels AS (
    SELECT
        telegram_channel_id,
        channel_name,
        ingestion_timestamp -- This column is needed for ordering in deduplication
    FROM
        {{ ref('stg_telegram_messages') }}
    WHERE
        telegram_channel_id IS NOT NULL -- Ensure we only process valid channel IDs
        AND channel_name IS NOT NULL -- Ensure we have a name for the channel
),

-- Deduplicate channels: If a channel appears multiple times,
-- select the record with the most recent ingestion timestamp.
deduplicated_channels AS (
    SELECT
        telegram_channel_id,
        channel_name,
        ROW_NUMBER() OVER (PARTITION BY telegram_channel_id ORDER BY ingestion_timestamp DESC) as rn
    FROM
        source_channels
)

SELECT
    -- Generate a surrogate key for the dimension table
    {{ dbt_utils.generate_surrogate_key(['telegram_channel_id']) }} AS channel_sk,
    telegram_channel_id,
    channel_name
FROM
    deduplicated_channels
WHERE
    rn = 1 -- This filters to keep only the latest record for each unique channel_telegram_id