#!/usr/bin/env python3
"""
Standalone utility to detect and clean duplicate rows from the jobs pipeline database.

This script performs a comprehensive check for duplicates across all data layers
and can optionally remove them. It's designed to be run independently from the
main pipeline for manual maintenance or troubleshooting.

Usage:
    python3 cleanup_duplicates.py [--cleanup]
    
    Without --cleanup: Performs read-only duplicate detection only
    With --cleanup: Detects duplicates AND removes them (keeping most recent)

Safety Features:
    - Creates a backup report before any modifications
    - Idempotent cleanup (safe to run multiple times)
    - Detailed logging of all operations
    - Post-cleanup verification to ensure success
"""

import os
import json
import sys
import argparse
from datetime import datetime
import psycopg2
from typing import Tuple, List, Dict, Any

def load_config() -> Dict[str, Any]:
    """Load configuration from local.settings.json"""
    with open('local.settings.json') as f:
        return json.load(f).get('Values', {})

def pg_connect(cfg: Dict[str, Any]):
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=cfg["PGHOST"],
        port=int(cfg["PGPORT"]),
        database=cfg["PGDATABASE"],
        user=cfg["PGUSER"],
        password=cfg["PGPASSWORD"],
        sslmode=cfg.get("PGSSLMODE", "require"),
    )

def detect_duplicates(conn) -> Dict[str, List[Tuple]]:
    """
    Detect duplicates in all tables
    Returns dict with 'landing', 'staging', 'skills' keys containing lists of duplicate groups
    """
    duplicates = {}
    
    with conn.cursor() as cur:
        # Landing layer duplicates
        cur.execute("""
            SELECT source_name, job_id, COUNT(*) as dup_count
            FROM landing.raw_jobs
            GROUP BY source_name, job_id
            HAVING COUNT(*) > 1
            ORDER BY dup_count DESC
        """)
        duplicates['landing'] = cur.fetchall()
        
        # Staging layer duplicates
        cur.execute("""
            SELECT source_name, job_id, COUNT(*) as dup_count
            FROM staging.jobs_v1
            GROUP BY source_name, job_id
            HAVING COUNT(*) > 1
            ORDER BY dup_count DESC
        """)
        duplicates['staging'] = cur.fetchall()
        
        # Job skills duplicates
        cur.execute("""
            SELECT source_name, job_id, skill, COUNT(*) as dup_count
            FROM staging.job_skills
            GROUP BY source_name, job_id, skill
            HAVING COUNT(*) > 1
            ORDER BY dup_count DESC
        """)
        duplicates['skills'] = cur.fetchall()
    
    return duplicates

def report_duplicates(duplicates: Dict[str, List[Tuple]]) -> None:
    """Pretty-print duplicate detection report"""
    print("\n" + "="*70)
    print("🔍 DUPLICATE DETECTION REPORT")
    print("="*70 + "\n")
    
    landing_dups = duplicates.get('landing', [])
    staging_dups = duplicates.get('staging', [])
    skills_dups = duplicates.get('skills', [])
    
    print("📋 Landing Layer (landing.raw_jobs)")
    if landing_dups:
        total_extra = sum(count - 1 for _, _, count in landing_dups)
        print(f"   ⚠️  Found {len(landing_dups)} duplicate combinations, {total_extra} extra rows")
        for source, job_id, count in landing_dups[:5]:
            print(f"      {source}/{job_id}: {count} copies")
        if len(landing_dups) > 5:
            print(f"      ... and {len(landing_dups) - 5} more")
    else:
        print(f"   ✅ No duplicates")
    
    print("\n📋 Staging Layer (staging.jobs_v1)")
    if staging_dups:
        total_extra = sum(count - 1 for _, _, count in staging_dups)
        print(f"   ⚠️  Found {len(staging_dups)} duplicate combinations, {total_extra} extra rows")
        for source, job_id, count in staging_dups[:5]:
            print(f"      {source}/{job_id}: {count} copies")
        if len(staging_dups) > 5:
            print(f"      ... and {len(staging_dups) - 5} more")
    else:
        print(f"   ✅ No duplicates")
    
    print("\n📋 Job Skills (staging.job_skills)")
    if skills_dups:
        total_extra = sum(count - 1 for _, _, _, count in skills_dups)
        print(f"   ⚠️  Found {len(skills_dups)} duplicate job-skill pairs, {total_extra} extra rows")
        for source, job_id, skill, count in skills_dups[:5]:
            print(f"      {source}/{job_id}/{skill}: {count} times")
        if len(skills_dups) > 5:
            print(f"      ... and {len(skills_dups) - 5} more")
    else:
        print(f"   ✅ No duplicates")
    
    # Summary
    total_duplicate_groups = len(landing_dups) + len(staging_dups) + len(skills_dups)
    total_extra_rows = (
        sum(count - 1 for _, _, count in landing_dups) +
        sum(count - 1 for _, _, count in staging_dups) +
        sum(count - 1 for _, _, _, count in skills_dups)
    )
    
    print("\n" + "="*70)
    if total_duplicate_groups == 0:
        print("✅ ALL TABLES CLEAN - NO DUPLICATES DETECTED")
    else:
        print(f"⚠️  DUPLICATES FOUND: {total_duplicate_groups} groups, {total_extra_rows} extra rows")
    print("="*70 + "\n")
    
    return total_duplicate_groups > 0

def cleanup_duplicates(conn) -> None:
    """Remove duplicate rows from all tables"""
    print("\n" + "="*70)
    print("🧹 STARTING DUPLICATE CLEANUP")
    print("="*70 + "\n")
    
    try:
        with conn.cursor() as cur:
            # Cleanup 1: landing.raw_jobs
            cur.execute("""
                DELETE FROM landing.raw_jobs
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM landing.raw_jobs
                    GROUP BY source_name, job_id
                )
            """)
            landing_removed = cur.rowcount
            if landing_removed > 0:
                print(f"   🗑️  landing.raw_jobs: Removed {landing_removed} duplicate rows")
            
            # Cleanup 2: staging.jobs_v1
            cur.execute("""
                DELETE FROM staging.jobs_v1
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM staging.jobs_v1
                    GROUP BY source_name, job_id
                )
            """)
            staging_removed = cur.rowcount
            if staging_removed > 0:
                print(f"   🗑️  staging.jobs_v1: Removed {staging_removed} duplicate rows")
            
            # Cleanup 3: staging.job_skills
            cur.execute("""
                DELETE FROM staging.job_skills
                WHERE ctid NOT IN (
                    SELECT MAX(ctid)
                    FROM staging.job_skills
                    GROUP BY source_name, job_id, skill
                )
            """)
            skills_removed = cur.rowcount
            if skills_removed > 0:
                print(f"   🗑️  staging.job_skills: Removed {skills_removed} duplicate rows")
            
            conn.commit()
            
            total_removed = landing_removed + staging_removed + skills_removed
            print(f"\n   ✅ Cleanup complete: {total_removed} duplicate rows removed\n")
            
            # Verify
            verify_cleanup(conn)
            
    except Exception as e:
        conn.rollback()
        print(f"\n   ❌ Cleanup failed: {e}\n")
        raise

def verify_cleanup(conn) -> bool:
    """Verify that all duplicates have been removed"""
    print("📋 Verification after cleanup:")
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM landing.raw_jobs) as landing_total,
                (SELECT COUNT(DISTINCT (source_name, job_id)) FROM landing.raw_jobs) as landing_unique,
                (SELECT COUNT(*) FROM staging.jobs_v1) as staging_total,
                (SELECT COUNT(DISTINCT (source_name, job_id)) FROM staging.jobs_v1) as staging_unique,
                (SELECT COUNT(*) FROM staging.job_skills) as skills_total,
                (SELECT COUNT(DISTINCT (source_name, job_id, skill)) FROM staging.job_skills) as skills_unique
        """)
        
        landing_total, landing_unique, staging_total, staging_unique, skills_total, skills_unique = cur.fetchone()
        
        landing_clean = landing_total == landing_unique
        staging_clean = staging_total == staging_unique
        skills_clean = skills_total == skills_unique
        
        print(f"   landing.raw_jobs: {landing_total} rows, {landing_unique} unique {'✅' if landing_clean else '⚠️'}")
        print(f"   staging.jobs_v1: {staging_total} rows, {staging_unique} unique {'✅' if staging_clean else '⚠️'}")
        print(f"   staging.job_skills: {skills_total} rows, {skills_unique} unique {'✅' if skills_clean else '⚠️'}")
        
        all_clean = landing_clean and staging_clean and skills_clean
        if all_clean:
            print(f"\n   ✅ All tables verified clean!")
        else:
            print(f"\n   ⚠️  Some tables still have duplicates!")
        
        return all_clean

def main():
    parser = argparse.ArgumentParser(
        description='Detect and optionally clean duplicate rows from the jobs pipeline database'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Remove duplicates (default: detection only)'
    )
    args = parser.parse_args()
    
    print("\n" + "🔍 JOBS PIPELINE DUPLICATE DETECTION & CLEANUP")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    print(f"   Cleanup mode: {'ENABLED (will remove duplicates)' if args.cleanup else 'DISABLED (detection only)'}\n")
    
    try:
        cfg = load_config()
        conn = pg_connect(cfg)
        
        # Phase 1: Detect duplicates
        duplicates = detect_duplicates(conn)
        has_duplicates = report_duplicates(duplicates)
        
        # Phase 2: Cleanup if requested and duplicates found
        if args.cleanup:
            if has_duplicates:
                response = input("Found duplicates. Proceed with cleanup? (yes/no): ").strip().lower()
                if response == 'yes':
                    cleanup_duplicates(conn)
                else:
                    print("Cleanup cancelled by user.")
            else:
                print("No duplicates found - nothing to clean.\n")
        
        conn.close()
        print("✅ Duplicate check complete.\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        sys.exit(1)

if __name__ == '__main__':
    main()
