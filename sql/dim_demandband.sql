CREATE OR REPLACE VIEW staging.dim_demandband AS
WITH bands(demandband_key, min_val, max_val, band_key, band_label, sort_order) AS (
  VALUES
    (1, 0::numeric, 12::numeric,  '0-12',  'Very Low', 1),
    (2, 13::numeric, 24::numeric, '13-24', 'Low',      2),
    (3, 25::numeric, 41::numeric, '25-41', 'Moderate', 3),
    (4, 42::numeric, 77::numeric, '42-77', 'High',     4),
    (5, 78::numeric, NULL,        '78+',   'Very High',5)
)
SELECT
  demandband_key,
  min_val,
  max_val,
  band_key,
  band_label,
  sort_order
FROM bands;