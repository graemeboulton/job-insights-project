CREATE OR REPLACE VIEW staging.dim_contract AS
WITH base AS (
    SELECT DISTINCT
        NULLIF(TRIM(contract_type), '') AS contract_type,
        COALESCE(full_time, FALSE)      AS full_time,
        COALESCE(part_time, FALSE)      AS part_time
    FROM staging.jobs_v1
)
, norm AS (
    SELECT
        LOWER(contract_type) AS contract_type,
        CASE
            WHEN full_time AND part_time THEN 'full & part-time'
            WHEN full_time THEN 'full-time'
            WHEN part_time THEN 'part-time'
            ELSE 'unspecified'
        END AS working_pattern
    FROM base
)
SELECT
    DENSE_RANK() OVER (ORDER BY contract_type, working_pattern) AS contract_key,
    contract_type,
    working_pattern
FROM norm;