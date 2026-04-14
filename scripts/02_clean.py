"""
02_clean.py
-----------
Clean positions and education datasets and save to cleaned/ as parquet.
Run on the SSH server — data stays in place, never copied off-server.

Usage:
    python scripts/02_clean.py
"""

import pandas as pd
import glob
import os

# ── Paths ───────────────────────────────────────────────────────────────────
POSITIONS_DIR = "/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/random_sample_1000/"
EDUCATION_FILE = "/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/user_education/user_education_0000_part_00.csv"
CLEANED_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/cleaned")
os.makedirs(CLEANED_DIR, exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD POSITIONS
# ══════════════════════════════════════════════════════════════════════════════
print("Loading positions files...")
all_files = sorted(glob.glob(os.path.join(POSITIONS_DIR, "*.csv")))
print(f"  Files found: {len(all_files)}")

pos = pd.concat(
    (pd.read_csv(f, engine="c", on_bad_lines="skip") for f in all_files),
    ignore_index=True,
)
print(f"  Raw positions shape: {pos.shape}")

# ── 1a. Cast user_id to string ──────────────────────────────────────────────
pos["user_id"] = pos["user_id"].astype(str)

# ── 1b. Replace "empty" with NaN ────────────────────────────────────────────
empty_before = (pos == "empty").sum().sum()
pos.replace("empty", pd.NA, inplace=True)
print(f"  Replaced {empty_before} 'empty' strings with NaN in positions")

# ── 1c. Drop description column ─────────────────────────────────────────────
if "description" in pos.columns:
    pos.drop(columns=["description"], inplace=True)
    print("  Dropped 'description' column")

# ── 1d. Deduplicate on user_id + position_id + company_name ─────────────────
dedup_cols = ["user_id", "position_id", "company_name"]
dupes = pos.duplicated(subset=dedup_cols, keep="first").sum()
if dupes > 0:
    pos.drop_duplicates(subset=dedup_cols, keep="first", inplace=True)
    print(f"  Removed {dupes} duplicate rows (same user_id + position_id + company_name)")
else:
    print(f"  No duplicates found on (user_id, position_id, company_name)")

# ── 1e. Drop rows with no employer identity at all ──────────────────────────
no_employer = pos[["company_name", "company_raw", "company_cleaned"]].isna().all(axis=1)
n_no_employer = no_employer.sum()
pos = pos[~no_employer]
print(f"  Dropped {n_no_employer} rows with no employer identity (company_name, company_raw, company_cleaned all null)")

# ── 1f. Recast numeric columns that became object after "empty" replacement ─
FLOAT_COLS = [
    "remote_suitability", "weight", "start_mean_sampled_salary",
    "end_mean_sampled_salary", "seniority", "salary", "rn", "rcid",
    "ultimate_parent_rcid",
]
for col in FLOAT_COLS:
    if col in pos.columns:
        pos[col] = pd.to_numeric(pos[col], errors="coerce")
print(f"  Recast {len(FLOAT_COLS)} numeric columns back to float")

# ── 1g. Parse dates ─────────────────────────────────────────────────────────
for col in ["startdate", "enddate"]:
    nulls_before = pos[col].isna().sum()
    pos[col] = pd.to_datetime(pos[col], errors="coerce")
    nat_after = pos[col].isna().sum()
    new_nats = nat_after - nulls_before
    print(f"  Parsed {col}: {nulls_before} nulls before → {nat_after} NaT after (silent parse failures: {new_nats})")

print(f"  Cleaned positions shape: {pos.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. LOAD EDUCATION
# ══════════════════════════════════════════════════════════════════════════════
print("\nLoading education file...")
edu = pd.read_csv(EDUCATION_FILE, low_memory=False)
print(f"  Raw education shape: {edu.shape}")

# ── 2a. Cast user_id to string ──────────────────────────────────────────────
edu["user_id"] = edu["user_id"].astype(str)

# ── 2b. Replace "empty" with NaN ────────────────────────────────────────────
empty_before = (edu == "empty").sum().sum()
edu.replace("empty", pd.NA, inplace=True)
print(f"  Replaced {empty_before} 'empty' strings with NaN in education")

# ── 2c. Parse dates ─────────────────────────────────────────────────────────
for col in ["startdate", "enddate"]:
    nulls_before = edu[col].isna().sum()
    edu[col] = pd.to_datetime(edu[col], errors="coerce")
    nat_after = edu[col].isna().sum()
    new_nats = nat_after - nulls_before
    print(f"  Parsed {col}: {nulls_before} nulls before → {nat_after} NaT after (silent parse failures: {new_nats})")

# ── 2d. Flag education rows missing both degree and field ───────────────────
edu["has_degree_info"] = edu["degree"].notna() | edu["field"].notna()
n_no_degree = (~edu["has_degree_info"]).sum()
print(f"  Flagged {n_no_degree} education rows with no degree or field info (kept, not dropped)")

print(f"  Cleaned education shape: {edu.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. SAVE TO INTERIM
# ══════════════════════════════════════════════════════════════════════════════
pos_path = os.path.join(CLEANED_DIR, "positions_clean.parquet")
edu_path = os.path.join(CLEANED_DIR, "education_clean.parquet")

pos.to_parquet(pos_path, index=False)
edu.to_parquet(edu_path, index=False)

print(f"\nSaved cleaned data:")
print(f"  {pos_path}  ({os.path.getsize(pos_path) / 1e6:.1f} MB)")
print(f"  {edu_path}  ({os.path.getsize(edu_path) / 1e6:.1f} MB)")
print("Done.")
