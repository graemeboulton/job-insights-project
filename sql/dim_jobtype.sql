CREATE OR REPLACE VIEW staging.dim_jobtype AS
WITH base AS (
  SELECT DISTINCT job_title
  FROM staging.jobs_v1
  WHERE NULLIF(TRIM(job_title), '') IS NOT NULL
),
mapped AS (
  SELECT job_title,
    CASE
      WHEN job_title ILIKE '%data engineer%'           THEN 'data engineering'
      WHEN job_title ILIKE '%machine learning%' 
        OR job_title ILIKE '%ml%'                      THEN 'ml/ai'
      WHEN job_title ILIKE '%scientist%'               THEN 'data science'
      WHEN job_title ILIKE '%analyst%'                 THEN 'analytics'
      WHEN job_title ILIKE '%bi %' OR job_title ILIKE '%power bi%' THEN 'bi/visualisation'
      WHEN job_title ILIKE '%architect%'               THEN 'architecture'
      ELSE 'other'
    END AS job_type
  FROM base
)
SELECT
  DENSE_RANK() OVER (ORDER BY job_type) AS jobtype_key,
  job_type
FROM (SELECT DISTINCT job_type FROM mapped) x;