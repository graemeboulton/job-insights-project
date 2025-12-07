CREATE OR REPLACE VIEW staging.dim_employer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY employer_id) AS employer_key,
    employer_id,
    employer_name AS employer_name
FROM (
    SELECT DISTINCT employer_id, employer_name
    FROM staging.jobs_v1
    WHERE employer_id IS NOT NULL
      AND employer_name IS NOT NULL
) d;