CREATE OR REPLACE VIEW staging.dim_salaryband AS
WITH base AS (
    SELECT DISTINCT
        GREATEST(0, COALESCE(salary_min,0)) AS smin,
        GREATEST(0, COALESCE(salary_max,0)) AS smax,
        COALESCE((salary_min + salary_max)/2.0, 0) AS savg
    FROM staging.jobs_v1
)
, nums AS (
    SELECT DISTINCT (FLOOR(GREATEST(savg, (smin + smax)/2.0)/10000.0)*10000)::int AS band_start
    FROM base
)
SELECT
    DENSE_RANK() OVER (ORDER BY band_start) AS salaryband_key,
    band_start,
    (band_start + 9999)                      AS band_end,
    CONCAT('£', band_start, '–£', band_start + 9999) AS band_label,
    DENSE_RANK() OVER (ORDER BY band_start)  AS sort_order
FROM nums
ORDER BY band_start;