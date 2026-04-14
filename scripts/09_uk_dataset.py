"""
09_uk_dataset.py
----------------
Project 2: Effects of blind hiring in UK civil service.

Filters the merged education + positions data to UK-only records.
Retains all columns for later narrowing.

Outputs:
  processed/uk_positions.parquet
  processed/uk_education.parquet
  processed/uk_merged.parquet

Run on the SSH server after 02_clean.py has produced cleaned parquets.

Usage:
    python scripts/09_uk_dataset.py
"""

import pandas as pd
import os

CLEANED_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/cleaned")
OUTPUT_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/processed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 1. Load cleaned data ────────────────────────────────────────────────────
print("Loading cleaned parquets...")
pos = pd.read_parquet(os.path.join(CLEANED_DIR, "positions_clean.parquet"))
edu = pd.read_parquet(os.path.join(CLEANED_DIR, "education_clean.parquet"))
print(f"  Positions: {pos.shape}")
print(f"  Education: {edu.shape}")

# ── 2. Filter positions to UK ───────────────────────────────────────────────
print("\nFiltering positions to United Kingdom...")
uk_pos = pos[pos["country"] == "United Kingdom"].copy()
print(f"  UK positions: {uk_pos.shape}")
print(f"  UK unique users: {uk_pos['user_id'].nunique()}")
print(f"  UK unique employers: {uk_pos['company_name'].nunique()}")

print("\n--- UK positions by region (top 20) ---")
print(uk_pos["region"].value_counts(dropna=False).head(20).to_string())

print("\n--- UK positions by state/area (top 20) ---")
print(uk_pos["state"].value_counts(dropna=False).head(20).to_string())

print("\n--- UK positions by job category ---")
print(uk_pos["job_category"].value_counts(dropna=False).to_string())

print("\n--- UK positions by seniority ---")
print(uk_pos["seniority"].value_counts(dropna=False).sort_index().to_string())

# ── 3. Filter education to UK users ─────────────────────────────────────────
# Keep education records for any user who has at least one UK position
uk_user_ids = set(uk_pos["user_id"].unique())
uk_edu = edu[edu["user_id"].isin(uk_user_ids)].copy()
print(f"\nEducation records for UK-positioned users: {uk_edu.shape}")
print(f"  Unique users with education data: {uk_edu['user_id'].nunique()}")

print("\n--- University country for UK workers (top 20) ---")
print(uk_edu["university_country"].value_counts(dropna=False).head(20).to_string())

print("\n--- Top 20 universities for UK workers ---")
print(uk_edu["university_name"].value_counts().head(20).to_string())

print("\n--- Degree distribution for UK workers ---")
print(uk_edu["degree"].value_counts(dropna=False).head(15).to_string())

# ── 4. Merge UK data ────────────────────────────────────────────────────────
print("\nMerging UK positions + education...")
uk_merged = pd.merge(uk_pos, uk_edu, on="user_id", how="inner",
                     suffixes=("_pos", "_edu"))
print(f"  Merged UK shape: {uk_merged.shape}")
print(f"  Merged unique users: {uk_merged['user_id'].nunique()}")

# ── 5. Save ──────────────────────────────────────────────────────────────────
uk_pos_path = os.path.join(OUTPUT_DIR, "uk_positions.parquet")
uk_edu_path = os.path.join(OUTPUT_DIR, "uk_education.parquet")
uk_merged_path = os.path.join(OUTPUT_DIR, "uk_merged.parquet")

uk_pos.to_parquet(uk_pos_path, index=False)
uk_edu.to_parquet(uk_edu_path, index=False)
uk_merged.to_parquet(uk_merged_path, index=False)

print(f"\nSaved:")
print(f"  {uk_pos_path} ({os.path.getsize(uk_pos_path) / 1024 / 1024:.1f} MB)")
print(f"  {uk_edu_path} ({os.path.getsize(uk_edu_path) / 1024 / 1024:.1f} MB)")
print(f"  {uk_merged_path} ({os.path.getsize(uk_merged_path) / 1024 / 1024:.1f} MB)")

# ── 6. Summary ───────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("UK DATASET SUMMARY")
print("=" * 60)
print(f"  UK positions:          {uk_pos.shape[0]:>10,} rows")
print(f"  UK education records:  {uk_edu.shape[0]:>10,} rows")
print(f"  UK merged:             {uk_merged.shape[0]:>10,} rows")
print(f"  Unique UK users:       {uk_pos['user_id'].nunique():>10,}")
print(f"  Unique UK employers:   {uk_pos['company_name'].nunique():>10,}")
print(f"  Users with education:  {uk_edu['user_id'].nunique():>10,}")
