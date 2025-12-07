-- Importance
CREATE OR REPLACE VIEW staging.dim_skill_importance AS
SELECT
  DENSE_RANK() OVER (ORDER BY lvl) AS importance_key,
  lvl                              AS importance_level
FROM (
  SELECT DISTINCT COALESCE(LOWER(NULLIF(TRIM(importance_level), '')), 'unknown') AS lvl
  FROM staging.job_skills
) s;

-- Proficiency
CREATE OR REPLACE VIEW staging.dim_skill_proficiency AS
SELECT
  DENSE_RANK() OVER (ORDER BY lvl) AS proficiency_key,
  lvl                              AS proficiency_level
FROM (
  SELECT DISTINCT COALESCE(LOWER(NULLIF(TRIM(proficiency_level), '')), 'unknown') AS lvl
  FROM staging.job_skills
) s;