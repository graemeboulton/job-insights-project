CREATE OR REPLACE VIEW staging.fact_job_skill AS
WITH base AS (
  SELECT
    js.job_id,
    js.source_name,
    js.extracted_at::timestamp                AS extracted_at,
    LOWER(TRIM(js.skill))                     AS skill_norm,
    COALESCE(LOWER(NULLIF(TRIM(js.importance_level), '')),  'unknown')   AS importance_norm,
    COALESCE(LOWER(NULLIF(TRIM(js.proficiency_level), '')), 'unknown')   AS proficiency_norm
  FROM staging.job_skills js
  WHERE NULLIF(TRIM(js.skill), '') IS NOT NULL
)
SELECT
  b.job_id,
  ds.skill_key,
  isi.importance_key,
  isp.proficiency_key,
  b.source_name,
  b.extracted_at
FROM base b
LEFT JOIN staging.dim_skill              ds  ON ds.skill_id = md5(b.skill_norm)
LEFT JOIN staging.dim_skill_importance  isi ON isi.importance_level = b.importance_norm
LEFT JOIN staging.dim_skill_proficiency isp ON isp.proficiency_level = b.proficiency_norm;