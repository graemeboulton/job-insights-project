CREATE OR REPLACE VIEW staging.dim_job_text AS
SELECT
    j.job_id,
    j.job_title,
    j.job_description
FROM staging.jobs_v1 j;