-- medical_insights_dwh/models/marts/fct_messages.sql
{{ config(materialized='table') }}

WITH stg_messages AS (
    SELECT * FROM {{ ref('stg_telegram_messages') }}
),
dim_channels AS (
    SELECT * FROM {{ ref('dim_channels') }}
),
dim_dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)
SELECT
    sm.message_pk,
    sm.message_id,
    -- Ensure channel_fk's type matches channel_sk's type (TEXT)
    COALESCE(dc.channel_sk, '-1') AS channel_fk, -- CORRECTED: Coalesce with a STRING '-1'
    COALESCE(dd_message_date.date_key, -1) AS message_date_fk,
    LENGTH(sm.message_content) AS message_length,
    CASE
        WHEN sm.media_type = 'photo' OR sm.media_type = 'document_image' THEN TRUE
        ELSE FALSE
    END AS has_image,
    sm.message_views,
    COALESCE(dd_scraped_date.date_key, -1) AS message_scraped_date_fk,
    sm.message_timestamp,
    sm.message_content
FROM stg_messages sm
LEFT JOIN dim_channels dc
    ON sm.channel_telegram_id = dc.channel_telegram_id
LEFT JOIN dim_dates dd_message_date
    ON sm.message_timestamp::DATE = dd_message_date.full_date
LEFT JOIN dim_dates dd_scraped_date
    ON sm.scraped_date = dd_scraped_date.full_date