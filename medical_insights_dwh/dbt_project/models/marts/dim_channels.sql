-- medical_insights_dwh/models/public/dim_channels.sql
{{ config(materialized='table') }}

WITH unique_channels_stg AS (
    SELECT
        channel_telegram_id,
        channel_name,
        -- Assign a row number to pick a consistent channel_name
        -- if there are multiple associated with the same channel_telegram_id.
        -- We order by channel_name ASC to ensure a deterministic choice.
        ROW_NUMBER() OVER (PARTITION BY channel_telegram_id ORDER BY channel_name ASC) as rn
    FROM {{ ref('stg_telegram_messages') }}
    WHERE channel_telegram_id IS NOT NULL -- Exclude any lingering NULLs, though you coalesce to -1 in staging
)
SELECT
    -- Generate surrogate key from the unique channel_telegram_id.
    -- This key will now be truly unique as channel_telegram_id itself is unique after filtering.
    {{ dbt_utils.generate_surrogate_key(['channel_telegram_id']) }} AS channel_sk,
    channel_telegram_id,
    channel_name
FROM unique_channels_stg
WHERE rn = 1 -- Select only one row for each unique channel_telegram_id