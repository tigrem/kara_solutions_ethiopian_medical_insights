-- medical_insights_dwh/models/staging/stg_telegram_messages.sql
{{ config(materialized='view') }}

WITH raw_messages_with_row_num AS (
    SELECT
        message_data,
        channel_name,
        scraped_date,
        ingestion_timestamp,
        COALESCE((message_data ->> 'id'), 'NULL_MESSAGE_ID') AS extracted_message_id,
        COALESCE((message_data -> 'peer_id' ->> 'channel_id')::BIGINT, -1) AS extracted_channel_telegram_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE((message_data ->> 'id'), 'NULL_MESSAGE_ID'),
                COALESCE((message_data -> 'peer_id' ->> 'channel_id')::BIGINT, -1),
                scraped_date
            ORDER BY
                ingestion_timestamp DESC
        ) as rn
    FROM {{ source('raw', 'raw_telegram_messages') }}
)

SELECT
    -- CORRECTED: Removed the inline comment from the generate_surrogate_key list
    {{ dbt_utils.generate_surrogate_key([
        'rm.extracted_message_id',
        'rm.extracted_channel_telegram_id',
        'rm.scraped_date'
    ]) }} AS message_pk,
    rm.extracted_message_id AS message_id,
    TO_TIMESTAMP(rm.message_data ->> 'date', 'YYYY-MM-DD"T"HH24:MI:SS') AS message_timestamp,
    rm.message_data ->> 'message' AS message_content,
    (rm.message_data ->> 'views')::BIGINT AS message_views,
    rm.extracted_channel_telegram_id AS channel_telegram_id,
    rm.channel_name,
    CASE
        WHEN rm.message_data -> 'media' IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_media,
    CASE
        WHEN rm.message_data -> 'media' ->> '_' = 'messageMediaPhoto' THEN 'photo'
        WHEN rm.message_data -> 'media' ->> '_' LIKE 'messageMediaDocument%' AND rm.message_data -> 'media' -> 'document' ->> 'mime_type' LIKE 'image%' THEN 'document_image'
        WHEN rm.message_data -> 'media' IS NOT NULL THEN 'other_media'
        ELSE NULL
    END AS media_type,
    rm.message_data -> 'media' -> 'document' ->> 'file_name' AS file_name,
    rm.message_data ->> 'file_path' AS file_path,
    rm.scraped_date,
    rm.ingestion_timestamp
FROM raw_messages_with_row_num rm
WHERE rm.rn = 1