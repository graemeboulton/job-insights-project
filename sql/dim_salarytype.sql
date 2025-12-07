CREATE OR REPLACE VIEW staging.dim_salarytype AS
SELECT
    DENSE_RANK() OVER (ORDER BY LOWER(salary_type)) AS salarytype_key,
    LOWER(salary_type) AS salary_type
FROM (
    SELECT DISTINCT salary_type
    FROM staging.jobs_v1
    WHERE NULLIF(TRIM(salary_type), '') IS NOT NULL
) d;