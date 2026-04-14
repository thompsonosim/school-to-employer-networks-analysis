"""
03_sanity_check.py
------------------
Validate the cleaned parquet files produced by 02_clean.py.
Run on the SSH server after 02_clean.py completes.

Usage:
    python scripts/03_sanity_check.py
"""

import pandas as pd
import os
import sys

CLEANED_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/cleaned")
pos_path = os.path.join(CLEANED_DIR, "positions_clean.parquet")
edu_path = os.path.join(CLEANED_DIR, "education_clean.parquet")

errors = []

# ── Load ────────────────────────────────────────────────────────────────────
print("Loading cleaned parquet files...")
pos = pd.read_parquet(pos_path)
edu = pd.read_parquet(edu_path)
print(f"  Positions: {pos.shape}")
print(f"  Education: {edu.shape}")

# ── Check 1: No "empty" strings remain ─────────────────────────────────────
print("\n[CHECK 1] No 'empty' strings remain")
pos_empty = (pos.astype(str) == "empty").sum().sum()
edu_empty = (edu.astype(str) == "empty").sum().sum()
if pos_empty > 0:
    errors.append(f"Positions still has {pos_empty} 'empty' strings")
if edu_empty > 0:
    errors.append(f"Education still has {edu_empty} 'empty' strings")
print(f"  Positions 'empty' count: {pos_empty}")
print(f"  Education 'empty' count: {edu_empty}")

# ── Check 2: user_id dtype consistent ──────────────────────────────────────
print("\n[CHECK 2] user_id dtype is string in both")
pos_uid = str(pos["user_id"].dtype)
edu_uid = str(edu["user_id"].dtype)
if pos_uid != edu_uid:
    errors.append(f"user_id dtype mismatch: positions={pos_uid}, education={edu_uid}")
print(f"  Positions user_id dtype: {pos_uid}")
print(f"  Education user_id dtype: {edu_uid}")

# ── Check 3: No description column ────────────────────────────────────────
print("\n[CHECK 3] 'description' column removed from positions")
if "description" in pos.columns:
    errors.append("'description' column still present in positions")
    print("  FAIL — column still present")
else:
    print("  OK")

# ── Check 4: Date columns are datetime ────────────────────────────────────
print("\n[CHECK 4] Date columns are datetime dtype")
for df, name in [(pos, "Positions"), (edu, "Education")]:
    for col in ["startdate", "enddate"]:
        dtype = str(df[col].dtype)
        ok = "datetime" in dtype
        if not ok:
            errors.append(f"{name}.{col} is {dtype}, expected datetime")
        print(f"  {name}.{col}: {dtype} {'OK' if ok else 'FAIL'}")

# ── Check 5: No rows with all-null employer ────────────────────────────────
print("\n[CHECK 5] No positions rows with all-null employer")
no_employer = pos[["company_name", "company_raw", "company_cleaned"]].isna().all(axis=1).sum()
if no_employer > 0:
    errors.append(f"{no_employer} positions rows have all-null employer")
    print(f"  FAIL — {no_employer} rows with no employer")
else:
    print("  OK")

# ── Check 6: has_degree_info flag exists ───────────────────────────────────
print("\n[CHECK 6] has_degree_info flag exists in education")
if "has_degree_info" not in edu.columns:
    errors.append("has_degree_info column missing from education")
    print("  FAIL — column missing")
else:
    n_true = edu["has_degree_info"].sum()
    n_false = (~edu["has_degree_info"]).sum()
    print(f"  OK — {n_true} with degree info, {n_false} without")

# ── Check 7: Column dtypes summary ────────────────────────────────────────
print("\n[INFO] Positions dtypes:")
print(pos.dtypes.to_string())
print(f"\n[INFO] Education dtypes:")
print(edu.dtypes.to_string())

# ── Summary ────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
if errors:
    print(f"SANITY CHECK FAILED — {len(errors)} error(s):")
    for e in errors:
        print(f"  ✗ {e}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED")
    sys.exit(0)
