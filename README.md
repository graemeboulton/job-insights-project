
# jobs-pipeline

An end-to-end portfolio project that pulls job postings daily from the Reed.co.uk API, cleans them with Python, loads to Azure PostgreSQL, and visualises trends in Power BI/Fabric.

## Key Features

- **ML-Powered Job Filtering**: Uses trained classifier to filter relevant job titles (>90% accuracy)
- **Incremental Data Loading**: Only fetches jobs posted in last N days (configurable)
- **Smart Description Enrichment**: Automatically fetches full descriptions from detail endpoint
- **Skill Extraction**: Extracts 82+ technical skills with categorization
- **Duplicate Prevention**: Atomic bulk inserts, ON CONFLICT handling, automatic detection
- **Data Quality Monitoring**: Truncation detection, enrichment rate tracking, duplicate detection
- **Expiry Management**: Automatically removes jobs past expiration date

## High-level architecture
```
[Timer Trigger Azure Function]
        │
        ▼
[Reed API] → [Pandas clean/transform] → [Azure PostgreSQL] → [Power BI/Fabric]
```


## Data Quality & Monitoring

Every pipeline run includes automatic monitoring:

### 1. **Enrichment Tracking**
```
📊 Enrichment stats: Total jobs=2238, Enriched=2231, Missing=0
   Enrichment rate: 99.7%
```

### 2. **Description Validation**
Detects truncated job descriptions and highlights Reed API limits:
```
📝 Description Length Validation:
   Range: 306-7072 chars, Avg: 2496 chars
   ⚠️  WARNING: 261 descriptions stuck at 453 chars (Reed API limit)
   Action: Re-run pipeline to retry detail endpoint calls
```

### 3. **Duplicate Detection**
Automatic check across all data layers:
```
🔍 Duplicate Detection Report:
   ✅ landing.raw_jobs: No duplicates
   ✅ staging.jobs_v1: No duplicates
   ✅ staging.job_skills: No duplicates
```

### 4. **Skill Extraction Reporting**
```
🎯 Skill extraction: Unique skills=82, Jobs with skills=2311, Total matches=5325
```

## Maintenance

### Manual Duplicate Detection & Cleanup

```bash
# Detection only (read-only)
python3 cleanup_duplicates.py

# Detection + cleanup (interactive)
python3 cleanup_duplicates.py --cleanup
```

See [`DUPLICATE_PREVENTION.md`](./DUPLICATE_PREVENTION.md) for detailed information about:
- Duplicate prevention architecture
- Root cause analysis of historical issues
- Safeguards and monitoring
- Best practices for developers

## Database Schema

### Landing Layer
- `landing.raw_jobs` - Raw JSON from Reed API search endpoint

### Staging Layer
- `staging.jobs_v1` - Flattened job records with extracted features
- `staging.job_skills` - Job-to-skill mappings with contextual metadata
- Various dimension views for analytics (dim_employer, dim_location, etc.)

### Core Layer
- `core.fact_job_posting` - Denormalized fact table for BI reporting
- `core.dim_company` - Company/employer dimension
- `core.dim_location` - Location dimension

## Configuration

Set environment variables in `local.settings.json`:

```json
{
  "Values": {
    "API_KEY": "your-reed-api-key",
    "PGHOST": "your-postgres-host",
    "PGDATABASE": "your-database",
    "PGUSER": "your-user",
    "PGPASSWORD": "your-password",
    "POSTED_BY_DAYS": "1",
    "MAX_RESULTS": "10000",
    "USE_ML_CLASSIFIER": "true",
    "ML_CLASSIFIER_THRESHOLD": "0.7"
  }
}
```

## Performance

| Metric | Value |
|--------|-------|
| Fetch 10,000 jobs | ~52 minutes |
| Skill extraction | ~45 seconds for 2,238 jobs |
| Database bulk insert | ~1 second for 2,238 rows |
| Duplicate detection | ~2 seconds |

## Status

✅ **Production Ready**
- All tables clean (0 duplicates)
- 99.7% enrichment rate
- Atomic upsert operations
- Automatic monitoring and alerts

Last updated: 2025-12-08

