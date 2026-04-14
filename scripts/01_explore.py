"""
01_explore.py
-------------
Initial exploration of positions and education datasets.
Run on the SSH server — data stays in place, never copied.

Usage:
    python scripts/01_explore.py
"""

import pandas as pd
import glob
import os

# ── Paths (source data, never modify these) ────────────────────────────────
POSITIONS_DIR = "/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/random_sample_1000/"
EDUCATION_FILE = "/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/user_education/user_education_0000_part_00.csv"

# ── 1. Load a single positions file to explore schema ─────────────────────
print("=" * 60)
print("POSITIONS — single file preview")
print("=" * 60)

pos_file = os.path.join(POSITIONS_DIR, "position_0000_part_00.csv")
pos = pd.read_csv(pos_file, engine='python')

print(f"Shape: {pos.shape}")
print(f"\nColumns ({len(pos.columns)}):")
for c in pos.columns:
    print(f"  {c}: {pos[c].dtype}")

print(f"\nNull counts:")
print(pos.isnull().sum().to_string())

print(f"\nSample rows (3):")
print(pos.head(3).to_string())

print(f"\nCountry distribution:")
print(pos["country"].value_counts().head(10).to_string())

print(f"\nJob category distribution:")
print(pos["job_category"].value_counts().head(10).to_string())

print(f"\nSeniority distribution:")
print(pos["seniority"].value_counts().to_string())

print(f"\nDate range (startdate):")
print(pd.to_datetime(pos["startdate"], errors="coerce").describe())

# ── 2. Load all sample positions files and concatenate ────────────────────
print("\n" + "=" * 60)
print("POSITIONS — all sample files combined")
print("=" * 60)

all_files = sorted(glob.glob(os.path.join(POSITIONS_DIR, "*.csv")))
print(f"Files found: {len(all_files)}")

pos_all = pd.concat(
    (pd.read_csv(f, engine='python') for f in all_files),
    ignore_index=True
)
print(f"Combined shape: {pos_all.shape}")
print(f"Unique users: {pos_all['user_id'].nunique()}")
print(f"Unique companies: {pos_all['company_name'].nunique()}")
print(f"Unique job categories: {pos_all['job_category'].nunique()}")

print(f"\nTop 10 countries:")
print(pos_all["country"].value_counts().head(10).to_string())

print(f"\nTop 10 job categories:")
print(pos_all["job_category"].value_counts().head(10).to_string())

print(f"\nSalary summary:")
print(pos_all[["start_mean_sampled_salary", "end_mean_sampled_salary", "salary"]].describe().to_string())

# ── 3. Load education file ─────────────────────────────────────────────────
print("\n" + "=" * 60)
print("EDUCATION — file preview")
print("=" * 60)

edu = pd.read_csv(EDUCATION_FILE, engine='python')
print(f"Shape: {edu.shape}")

print(f"\nColumns ({len(edu.columns)}):")
for c in edu.columns:
    print(f"  {c}: {edu[c].dtype}")

print(f"\nNull counts:")
print(edu.isnull().sum().to_string())

print(f"\nTop 10 universities:")
print(edu["university_name"].value_counts().head(10).to_string())

print(f"\nTop 10 degrees:")
print(edu["degree"].value_counts().head(10).to_string())

print(f"\nTop 10 fields:")
print(edu["field"].value_counts().head(10).to_string())

print(f"\nUniversity country distribution:")
print(edu["university_country"].value_counts().head(10).to_string())

# ── 4. Join: education → positions via user_id ────────────────────────────
print("\n" + "=" * 60)
print("JOIN — education + positions via user_id")
print("=" * 60)

merged = pd.merge(
    edu[["user_id", "university_name", "degree", "field", "world_rank", "university_country"]],
    pos_all[["user_id", "company_name", "job_category", "mapped_role", "country", "seniority", "salary", "startdate", "enddate"]],
    on="user_id",
    how="inner"
)
print(f"Merged shape: {merged.shape}")
print(f"Unique users in both datasets: {merged['user_id'].nunique()}")

print(f"\nTop university → job category flows:")
print(
    merged.groupby(["university_name", "job_category"])
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .head(15)
    .to_string(index=False)
)

print(f"\nTop degree → job category flows:")
print(
    merged.groupby(["degree", "job_category"])
    .size()
    .reset_index(name="count")
    .sort_values("count", ascending=False)
    .head(15)
    .to_string(index=False)
)

print("\nDone.")
