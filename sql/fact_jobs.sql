CREATE OR REPLACE VIEW staging.fact_jobs AS
WITH base AS (
    SELECT
        j.staging_id,
        j.source_name,
        j.job_id,
        j.job_title,
        j.employer_id,
        j.employer_name,
        j.location_name,
        j.work_location_type,
        j.salary_min,
        j.salary_max,

        -- Derived salary_average
        COALESCE((j.salary_min + j.salary_max) / 2.0, NULL) AS salary_average,

        j.salary_type,
        j.applications,
        j.job_url,
        j.job_description,
        j.posted_at,
        j.expires_at,
        j.ingested_at,
        j.updated_at,
        j.seniority_level,
        j.contract_type,
        COALESCE(j.full_time, FALSE) AS full_time,
        COALESCE(j.part_time, FALSE) AS part_time,

        -- Natural keys for joining to dimensions
        md5(LOWER(TRIM(j.location_name)) || '|' || LOWER(TRIM(j.work_location_type))) AS location_id_nk,
        LOWER(COALESCE(NULLIF(TRIM(j.source_name), ''), 'reed'))                       AS source_name_nk,
        LOWER(NULLIF(TRIM(j.seniority_level), ''))                                     AS seniority_label_nk,
        LOWER(NULLIF(TRIM(j.contract_type), ''))                                       AS contract_type_nk,
        CASE
            WHEN COALESCE(j.full_time, FALSE) AND COALESCE(j.part_time, FALSE) THEN 'full & part-time'
            WHEN COALESCE(j.full_time, FALSE) THEN 'full-time'
            WHEN COALESCE(j.part_time, FALSE) THEN 'part-time'
            ELSE 'unspecified'
        END AS working_pattern_nk,
        LOWER(NULLIF(TRIM(j.salary_type), ''))                                         AS salary_type_nk
    FROM staging.jobs_v1 j
),
derived AS (
    SELECT
        b.*,

        -- Normalised dates
        b.posted_at::date  AS posted_date,
        b.expires_at::date AS expires_date,

        -- Days open (integer difference)
        GREATEST(0, (CURRENT_DATE - b.posted_at::date)) AS days_open,

        -- Days until expiry (integer)
        (b.expires_at::date - CURRENT_DATE) AS days_to_expiry,

        -- Status flags
        (b.expires_at::date >= CURRENT_DATE) AS is_active,
        (
            b.expires_at::date >= CURRENT_DATE
            AND b.expires_at::date <= CURRENT_DATE + INTERVAL '3 day'
        ) AS is_ending_soon,

        -- Applications per day (integer-safe)
        (b.applications::numeric / GREATEST(1, (CURRENT_DATE - b.posted_at::date))) AS apps_per_day
    FROM base b
),
with_keys AS (
    SELECT
        d.*,

        -- Surrogate dimension keys
        de.employer_key,
        dl.location_key,
        ds.source_key,
        dsn.seniority_key,
        dc.contract_key,
        dst.salarytype_key,
        sb.salaryband_key,
        ab.ageband_key,

        -- Demand band based on RAW application counts (matching your PQ thresholds)
        db.demandband_key,

        -- Derived UX helper flag
        (d.is_ending_soon AND db.demandband_key IN (1,2)) AS is_low_competition_ending_soon

    FROM derived d
    LEFT JOIN staging.dim_employer   de  ON de.employer_id      = d.employer_id
    LEFT JOIN staging.dim_location   dl  ON dl.location_id      = d.location_id_nk
    LEFT JOIN staging.dim_source     ds  ON ds.source_name      = d.source_name_nk
    LEFT JOIN staging.dim_seniority  dsn ON dsn.seniority_label = d.seniority_label_nk
    LEFT JOIN staging.dim_contract   dc  ON dc.contract_type    = d.contract_type_nk
                                         AND dc.working_pattern = d.working_pattern_nk
    LEFT JOIN staging.dim_salarytype dst ON dst.salary_type     = d.salary_type_nk

    -- Salary band: average falls within band_start/band_end
    LEFT JOIN staging.dim_salaryband sb  
           ON d.salary_average BETWEEN sb.band_start AND sb.band_end

    -- Age band
    LEFT JOIN staging.dim_ageband ab
           ON d.days_open BETWEEN ab.min_days AND ab.max_days

    -- Demand band based on raw applications
    LEFT JOIN staging.dim_demandband db
           ON d.applications >= db.min_val
          AND (db.max_val IS NULL OR d.applications <= db.max_val)
)

SELECT
    -- Degenerate identifiers
    job_id,
    job_title,
    job_url,

    -- Natural keys (kept for debugging)
    employer_id,
    location_id_nk   AS location_id,
    source_name_nk   AS source_name,
    seniority_label_nk AS seniority_label,
    contract_type_nk AS contract_type,
    working_pattern_nk AS working_pattern,
    salary_type_nk   AS salary_type,

    -- Surrogate keys for Power BI relationships
    employer_key,
    location_key,
    source_key,
    seniority_key,
    contract_key,
    salarytype_key,
    salaryband_key,
    ageband_key,
    demandband_key,

    -- Measures
    salary_min,
    salary_max,
    salary_average,
    applications,
    apps_per_day,
    days_open,
    days_to_expiry,
    is_active,
    is_ending_soon,
    is_low_competition_ending_soon,

    -- Dates
    posted_date,
    expires_date,
    ingested_at,
    updated_at,

    -- Additional reference columns (hide in PBI if desired)
    staging_id,
    employer_name,
    location_name,
    work_location_type,
    job_description

FROM with_keys;