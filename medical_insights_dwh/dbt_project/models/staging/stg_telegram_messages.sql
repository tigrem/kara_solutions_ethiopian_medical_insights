-- models/staging/stg_telegram_messages.sql

WITH source_data AS (
    SELECT
        id, -- This is the internal ID of the raw record
        message_data,
        channel_name,
        scraped_date,
        ingestion_timestamp
    FROM
        {{ source('raw', 'raw_telegram_messages') }}
)

SELECT
    id AS raw_id, -- Renaming the raw table's ID to avoid confusion
    -- Extracting fields from the message_data JSONB column
    (message_data->>'id')::VARCHAR AS message_id,
    (message_data->>'date')::TIMESTAMP WITH TIME ZONE AS message_timestamp,
    (message_data->>'views')::INTEGER AS views_count,
    (message_data->>'message')::TEXT AS message_text,
    (message_data->>'file_name')::VARCHAR AS file_name,
    (message_data->>'file_path')::TEXT AS file_path,
    (message_data->>'has_media')::BOOLEAN AS has_media,
    (message_data->>'channel_id')::BIGINT AS telegram_channel_id,
    (message_data->>'media_type')::VARCHAR AS media_type,

    -- Other directly available columns
    channel_name,
    scraped_date,
    ingestion_timestamp
FROM
    source_data