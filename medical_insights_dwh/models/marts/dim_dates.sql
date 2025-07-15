{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT
        GENERATE_SERIES(
            '2023-01-01'::DATE, -- Start date, adjust if your data goes back further
            CURRENT_DATE + INTERVAL '1 year', -- End date, extend as needed
            '1 day'::INTERVAL
        ) AS full_date
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INT AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DOW FROM full_date) AS day_of_week, -- 0=Sunday, 6=Saturday
    TRIM(TO_CHAR(full_date, 'Day')) AS day_name,
    TRIM(TO_CHAR(full_date, 'Month')) AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(WEEK FROM full_date) AS week_of_year
FROM date_spine
ORDER BY full_date