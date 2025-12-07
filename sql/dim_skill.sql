CREATE OR REPLACE VIEW staging.dim_skill AS
SELECT
  DENSE_RANK() OVER (ORDER BY LOWER(skill_name)) AS skill_key,
  LOWER(skill_name) AS skill_name
FROM (
  SELECT DISTINCT skill_name
  FROM staging.job_skills
  WHERE NULLIF(TRIM(skill_name), '') IS NOT NULL
) d;

CREATE OR REPLACE VIEW staging.fact_job_skill AS
SELECT
  j.job_id,
  ds.skill_key
FROM staging.job_skills s
JOIN staging.dim_skill ds ON LOWER(ds.skill_name) = LOWER(s.skill_name)
JOIN staging.jobs_v1 j    ON j.job_id = s.job_id;