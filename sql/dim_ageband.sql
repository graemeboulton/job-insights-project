CREATE OR REPLACE VIEW staging.dim_ageband AS
WITH def AS (
    SELECT 0 AS min_days, 3 AS max_days,  '0–3 days'  AS label, 1 AS sort_order UNION ALL
    SELECT 4, 7,  '4–7 days',   2 UNION ALL
    SELECT 8, 14, '8–14 days',  3 UNION ALL
    SELECT 15,30, '15–30 days', 4 UNION ALL
    SELECT 31,9999,'31+ days',  5
)
SELECT
    sort_order AS ageband_key,
    label      AS ageband_label,
    min_days,
    max_days,
    sort_order
FROM def;