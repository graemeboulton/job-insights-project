CREATE OR REPLACE VIEW staging.dim_source AS
SELECT
    DENSE_RANK() OVER (ORDER BY LOWER(source_name)) AS source_key,
    LOWER(source_name) AS source_name
FROM (
    SELECT DISTINCT COALESCE(NULLIF(TRIM(source_name), ''), 'reed') AS source_name
    FROM staging.jobs_v1
) d;