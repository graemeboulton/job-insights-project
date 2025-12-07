CREATE OR REPLACE VIEW staging.dim_seniority AS
SELECT
    DENSE_RANK() OVER (ORDER BY LOWER(seniority_level)) AS seniority_key,
    LOWER(seniority_level) AS seniority_label
FROM (
    SELECT DISTINCT seniority_level
    FROM staging.jobs_v1
    WHERE NULLIF(TRIM(seniority_level), '') IS NOT NULL
) d;