"""
04_analyse.py
-------------
Analysis: Does university ranking affect salary outcomes?
- Descriptive: salary by university rank quartile, by degree
- Interaction: does degree × rank affect salary?
- Data linkage quality assessment
- Preparation for Imbens synthetic control

Run on the SSH server after 02_clean.py has produced cleaned parquets.

Usage:
    python scripts/04_analyse.py
"""

import pandas as pd
import numpy as np
import os

CLEANED_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/cleaned")

# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD CLEANED DATA
# ══════════════════════════════════════════════════════════════════════════════
print("Loading cleaned parquets...")
pos = pd.read_parquet(os.path.join(CLEANED_DIR, "positions_clean.parquet"))
edu = pd.read_parquet(os.path.join(CLEANED_DIR, "education_clean.parquet"))
print(f"  Positions: {pos.shape}")
print(f"  Education: {edu.shape}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. MERGE — education + positions via user_id
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("MERGE — education + positions")
print("=" * 60)

merged = pd.merge(
    edu[["user_id", "university_name", "degree", "field", "world_rank",
         "university_country", "has_degree_info"]],
    pos[["user_id", "company_name", "ultimate_parent_company_name",
         "job_category", "mapped_role", "country", "seniority", "salary",
         "startdate", "enddate"]],
    on="user_id",
    how="inner",
)
print(f"Merged shape: {merged.shape}")
print(f"Unique users: {merged['user_id'].nunique()}")

# ── Filter to rows with both salary and world_rank ──────────────────────────
has_salary = merged["salary"].notna()
has_rank = merged["world_rank"].notna()
print(f"\nRows with salary: {has_salary.sum()} ({has_salary.mean():.1%})")
print(f"Rows with world_rank: {has_rank.sum()} ({has_rank.mean():.1%})")
print(f"Rows with BOTH salary + rank: {(has_salary & has_rank).sum()}")

analysis = merged[has_salary & has_rank].copy()
print(f"Analysis subset shape: {analysis.shape}")
print(f"Analysis subset unique users: {analysis['user_id'].nunique()}")

if analysis.shape[0] == 0:
    print("\n⚠ No rows have both salary and world_rank — cannot proceed.")
    print("This means the join between education and positions yields too few")
    print("matched users with ranked universities. Check data linkage.")
    exit(1)

# ══════════════════════════════════════════════════════════════════════════════
# 3. DATA LINKAGE QUALITY
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("DATA LINKAGE QUALITY")
print("=" * 60)

total_pos_users = pos["user_id"].nunique()
total_edu_users = edu["user_id"].nunique()
matched_users = merged["user_id"].nunique()
analysis_users = analysis["user_id"].nunique()

print(f"Position users:         {total_pos_users:>10,}")
print(f"Education users:        {total_edu_users:>10,}")
print(f"Matched (inner join):   {matched_users:>10,} ({matched_users/total_pos_users:.1%} of pos users)")
print(f"With salary + rank:     {analysis_users:>10,} ({analysis_users/matched_users:.1%} of matched)")

print(f"\nworld_rank distribution in analysis set:")
print(analysis["world_rank"].describe().to_string())

# ══════════════════════════════════════════════════════════════════════════════
# 4. UNIVERSITY RANK QUARTILES
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SALARY BY UNIVERSITY RANK QUARTILE")
print("=" * 60)

analysis["rank_quartile"] = pd.qcut(
    analysis["world_rank"], q=4,
    labels=["Q1 (top)", "Q2", "Q3", "Q4 (bottom)"],
)

rank_salary = (
    analysis.groupby("rank_quartile", observed=True)["salary"]
    .agg(["count", "mean", "median", "std"])
    .round(0)
)
print(rank_salary.to_string())

print(f"\nMean salary difference (Q1 top vs Q4 bottom):")
q1_mean = rank_salary.loc["Q1 (top)", "mean"]
q4_mean = rank_salary.loc["Q4 (bottom)", "mean"]
print(f"  Q1 (top-ranked):    ${q1_mean:,.0f}")
print(f"  Q4 (bottom-ranked): ${q4_mean:,.0f}")
print(f"  Difference:         ${q1_mean - q4_mean:,.0f} ({(q1_mean - q4_mean) / q4_mean:.1%})")

# ══════════════════════════════════════════════════════════════════════════════
# 5. SALARY BY DEGREE
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SALARY BY DEGREE")
print("=" * 60)

degree_salary = (
    analysis.groupby("degree", observed=True)["salary"]
    .agg(["count", "mean", "median", "std"])
    .sort_values("mean", ascending=False)
    .round(0)
)
print(degree_salary.head(10).to_string())

# ══════════════════════════════════════════════════════════════════════════════
# 6. INTERACTION: DEGREE × RANK QUARTILE → SALARY
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("DEGREE × RANK QUARTILE → MEAN SALARY")
print("=" * 60)

interaction = (
    analysis.groupby(["degree", "rank_quartile"], observed=True)["salary"]
    .agg(["count", "mean"])
    .round(0)
)
# Filter to degree groups with enough observations
interaction = interaction[interaction["count"] >= 10]
print(interaction.to_string())

# ── Same degree, top vs bottom rank ─────────────────────────────────────────
print("\n--- Salary gap: same degree, top-ranked vs bottom-ranked university ---")
pivot = (
    analysis.groupby(["degree", "rank_quartile"], observed=True)["salary"]
    .mean()
    .unstack("rank_quartile")
)
pivot["gap_Q1_vs_Q4"] = pivot.get("Q1 (top)", 0) - pivot.get("Q4 (bottom)", 0)
pivot["gap_pct"] = (pivot["gap_Q1_vs_Q4"] / pivot.get("Q4 (bottom)", 1) * 100).round(1)
print(pivot[["Q1 (top)", "Q4 (bottom)", "gap_Q1_vs_Q4", "gap_pct"]].dropna().round(0).to_string())

# ══════════════════════════════════════════════════════════════════════════════
# 7. TOP UNIVERSITIES BY SALARY
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("TOP 20 UNIVERSITIES BY MEDIAN SALARY (min 10 observations)")
print("=" * 60)

uni_salary = (
    analysis.groupby("university_name", observed=True)["salary"]
    .agg(["count", "mean", "median"])
    .round(0)
)
uni_salary = uni_salary[uni_salary["count"] >= 10]
print(uni_salary.sort_values("median", ascending=False).head(20).to_string())

# ══════════════════════════════════════════════════════════════════════════════
# 8. SALARY BY SENIORITY × RANK QUARTILE
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SALARY BY SENIORITY × RANK QUARTILE")
print("=" * 60)

sen_rank = (
    analysis.groupby(["seniority", "rank_quartile"], observed=True)["salary"]
    .agg(["count", "mean"])
    .round(0)
)
sen_rank = sen_rank[sen_rank["count"] >= 5]
print(sen_rank.to_string())

# ══════════════════════════════════════════════════════════════════════════════
# 9. SYNTHETIC CONTROL PREPARATION
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SYNTHETIC CONTROL — PREPARATION")
print("=" * 60)

# Treatment: graduates from top-100 world-ranked universities
# Control pool: graduates from universities ranked 101+
# Outcome: salary
# Covariates: degree, field, job_category, seniority

analysis["treated"] = analysis["world_rank"] <= 100
n_treated = analysis["treated"].sum()
n_control = (~analysis["treated"]).sum()
print(f"Treatment group (rank <= 100):  {n_treated:,} observations")
print(f"Control pool (rank > 100):      {n_control:,} observations")

print(f"\nTreatment group — covariate summary:")
for col in ["degree", "job_category", "seniority"]:
    print(f"\n  {col}:")
    print(analysis[analysis["treated"]][col].value_counts().head(5).to_string())

print(f"\nControl group — covariate summary:")
for col in ["degree", "job_category", "seniority"]:
    print(f"\n  {col}:")
    print(analysis[~analysis["treated"]][col].value_counts().head(5).to_string())

# ── Raw salary comparison ───────────────────────────────────────────────────
treated_salary = analysis.loc[analysis["treated"], "salary"]
control_salary = analysis.loc[~analysis["treated"], "salary"]
print(f"\nRaw salary comparison:")
print(f"  Treated mean:  ${treated_salary.mean():,.0f}")
print(f"  Control mean:  ${control_salary.mean():,.0f}")
print(f"  Difference:    ${treated_salary.mean() - control_salary.mean():,.0f}")

# ── Covariate balance table (for synthetic control matching) ────────────────
print(f"\nCovariate balance (treatment vs control):")
for col in ["seniority"]:
    t_mean = analysis.loc[analysis["treated"], col].mean()
    c_mean = analysis.loc[~analysis["treated"], col].mean()
    print(f"  {col}: treated={t_mean:.2f}, control={c_mean:.2f}, diff={t_mean - c_mean:.2f}")

# Degree distribution comparison
print(f"\n  Degree distribution (% of group):")
t_deg = analysis[analysis["treated"]]["degree"].value_counts(normalize=True).head(5) * 100
c_deg = analysis[~analysis["treated"]]["degree"].value_counts(normalize=True).head(5) * 100
deg_compare = pd.DataFrame({"treated_%": t_deg, "control_%": c_deg}).round(1)
print(deg_compare.to_string())

print(f"\n{'=' * 60}")
print("NEXT STEPS for synthetic control (05_synthetic_control.py):")
print("=" * 60)
print("""
1. Install SparseSC or implement Imbens (2015) synthetic control:
     pip install SparseSC   (or use scipy.optimize for manual weights)
2. Unit of analysis: university (aggregate salary by university)
3. Treatment: top-100 ranked university
4. Donor pool: universities ranked 101+
5. Match on: degree mix, field mix, job_category mix, mean seniority
6. Outcome: mean/median salary of graduates
7. Key question: after matching on observable characteristics,
   does a top-100 university still yield higher salaries?
""")

print("Done.")
