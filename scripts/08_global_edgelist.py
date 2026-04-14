"""
08_global_edgelist.py
---------------------
Project 1: School-to-employer pipelines around the world.

Collapses the merged education + positions data into an edgelist where:
  - Each row is a (university, employer) pair
  - A count column records how many individuals link that pair
Covers all major countries in the data.

Outputs:
  processed/global_edgelist.csv

Run on the SSH server after 02_clean.py has produced cleaned parquets.

Usage:
    python scripts/08_global_edgelist.py
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

# ── 2. Merge on user_id ─────────────────────────────────────────────────────
print("\nMerging education + positions on user_id (inner join)...")
merged = pd.merge(
    edu[["user_id", "university_name", "university_country"]],
    pos[["user_id", "company_name", "ultimate_parent_company_name", "country"]],
    on="user_id",
    how="inner",
)
print(f"  Merged shape: {merged.shape}")
print(f"  Unique users: {merged['user_id'].nunique()}")

# ── 3. Filter to rows with valid university and employer ─────────────────────
merged = merged.dropna(subset=["university_name", "company_name"])
print(f"  After dropping null university/employer: {merged.shape}")

# ── 4. Build edgelist ────────────────────────────────────────────────────────
print("\nBuilding edgelist (university → employer pairs)...")
edgelist = (
    merged
    .groupby(["university_name", "company_name", "university_country", "country"])
    .agg(count=("user_id", "nunique"))
    .reset_index()
    .sort_values("count", ascending=False)
)

edgelist.rename(columns={
    "university_name": "graduate_institution",
    "company_name": "employer",
    "university_country": "institution_country",
    "country": "employer_country",
}, inplace=True)

print(f"  Edgelist shape: {edgelist.shape}")
print(f"  Unique institutions: {edgelist['graduate_institution'].nunique()}")
print(f"  Unique employers: {edgelist['employer'].nunique()}")
print(f"  Unique institution countries: {edgelist['institution_country'].nunique()}")
print(f"  Unique employer countries: {edgelist['employer_country'].nunique()}")

# ── 5. Summary stats ────────────────────────────────────────────────────────
print("\n--- Top 20 school-to-employer pipelines ---")
print(edgelist.head(20).to_string(index=False))

print("\n--- Pairs by institution country (top 20) ---")
print(edgelist.groupby("institution_country")["count"].sum()
      .sort_values(ascending=False).head(20).to_string())

print("\n--- Pairs by employer country (top 20) ---")
print(edgelist.groupby("employer_country")["count"].sum()
      .sort_values(ascending=False).head(20).to_string())

print(f"\n--- Count distribution ---")
print(edgelist["count"].describe().to_string())

# ── 6. Save ──────────────────────────────────────────────────────────────────
out_path = os.path.join(OUTPUT_DIR, "global_edgelist.csv")
edgelist.to_csv(out_path, index=False)
print(f"\nSaved edgelist to {out_path}")
print(f"  File size: {os.path.getsize(out_path) / 1024 / 1024:.1f} MB")
