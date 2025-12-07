CREATE OR REPLACE VIEW staging.dim_location AS
WITH base AS (
    SELECT DISTINCT
        NULLIF(TRIM(location_name), '')      AS location_name,
        NULLIF(TRIM(work_location_type), '') AS work_location_type
    FROM staging.jobs_v1
    WHERE NULLIF(TRIM(location_name), '') IS NOT NULL
)
, keyed AS (
    SELECT
        md5(LOWER(location_name) || '|' || LOWER(work_location_type)) AS location_id,
        INITCAP(location_name) AS location_name,
        work_location_type
    FROM base
)
SELECT
    DENSE_RANK() OVER (ORDER BY location_id) AS location_key,
    location_id,
    location_name,
    work_location_type
FROM keyed;