-- models/marts/fct_image_detections.sql
{{ config(materialized='table') }}

WITH yolo_detections AS (
    SELECT
        CAST(message_id AS VARCHAR) AS message_id,
        detected_object_class,
        confidence_score,
        CAST(detection_timestamp AS TIMESTAMP) AS detection_timestamp
    FROM {{ source('raw', 'yolo_detections_csv') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['message_id', 'detected_object_class', 'detection_timestamp']) }} AS image_detection_pk,
    yd.message_id,
    yd.detected_object_class,
    yd.confidence_score,
    yd.detection_timestamp
FROM yolo_detections yd
-- LEFT JOIN {{ ref('fct_messages') }} fm ON yd.message_id = fm.message_pk