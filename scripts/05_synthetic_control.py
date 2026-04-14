"""
05_synthetic_control.py
-----------------------
Imbens-style synthetic control: does attending a top-100 ranked university
cause higher salary outcomes, after matching on observable characteristics?

Method (Abadie, Diamond, Hainmueller 2010 / Imbens 2015):
- Unit of analysis: university (aggregated from individual data)
- Treatment: top-100 world-ranked university
- Donor pool: universities ranked 101+
- Covariates: degree mix, field mix, job_category mix, mean seniority
- Outcome: mean salary of graduates
- Weights: non-negative, sum to 1, found via constrained optimisation
- Inference: placebo tests (apply method to each donor uni)

Dependencies: pandas, numpy, scipy (all in standard scientific Python)

Usage:
    python scripts/05_synthetic_control.py
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize
import os
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

CLEANED_DIR = os.path.expanduser("~/school-to-employer-networks-analysis/cleaned")
MIN_OBS_PER_UNI = 10  # minimum graduates to include a university

# ══════════════════════════════════════════════════════════════════════════════
# 1. LOAD AND MERGE
# ══════════════════════════════════════════════════════════════════════════════
print("Loading cleaned data...")
pos = pd.read_parquet(os.path.join(CLEANED_DIR, "positions_clean.parquet"))
edu = pd.read_parquet(os.path.join(CLEANED_DIR, "education_clean.parquet"))

merged = pd.merge(
    edu[["user_id", "university_name", "degree", "field", "world_rank",
         "university_country", "has_degree_info"]],
    pos[["user_id", "company_name", "ultimate_parent_company_name",
         "job_category", "seniority", "salary"]],
    on="user_id",
    how="inner",
)

# Filter to rows with salary and world_rank
analysis = merged[merged["salary"].notna() & merged["world_rank"].notna()].copy()
print(f"Analysis set: {analysis.shape[0]} observations, {analysis['user_id'].nunique()} users")

# ══════════════════════════════════════════════════════════════════════════════
# 2. AGGREGATE TO UNIVERSITY LEVEL
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("AGGREGATING TO UNIVERSITY LEVEL")
print("=" * 60)


def build_uni_features(df):
    """Aggregate individual-level data to university-level covariates."""
    records = []
    for uni, group in df.groupby("university_name"):
        if len(group) < MIN_OBS_PER_UNI:
            continue

        row = {"university_name": uni}
        row["n_obs"] = len(group)
        row["mean_salary"] = group["salary"].mean()
        row["median_salary"] = group["salary"].median()
        row["mean_seniority"] = group["seniority"].mean()
        row["world_rank"] = group["world_rank"].iloc[0]

        # Degree distribution (proportions)
        deg_dist = group["degree"].value_counts(normalize=True)
        for d in ["Bachelor", "Master", "MBA", "Doctor"]:
            row[f"pct_{d}"] = deg_dist.get(d, 0.0)

        # Job category distribution (proportions)
        jc_dist = group["job_category"].value_counts(normalize=True)
        for jc in ["Engineer", "Sales", "Admin", "Marketing", "Finance", "Scientist", "Operations"]:
            row[f"pct_{jc}"] = jc_dist.get(jc, 0.0)

        records.append(row)

    return pd.DataFrame(records)


uni_df = build_uni_features(analysis)
print(f"Universities with >= {MIN_OBS_PER_UNI} observations: {len(uni_df)}")

# Split into treated and donor
treated_df = uni_df[uni_df["world_rank"] <= 100].copy()
donor_df = uni_df[uni_df["world_rank"] > 100].copy()
print(f"Treated universities (rank <= 100): {len(treated_df)}")
print(f"Donor universities (rank > 100):    {len(donor_df)}")

if len(treated_df) == 0:
    print("\n⚠ No treated universities with >= 10 observations.")
    print("Try lowering MIN_OBS_PER_UNI or using the full positions dataset.")
    exit(1)

if len(donor_df) < 2:
    print("\n⚠ Not enough donor universities for synthetic control.")
    exit(1)

print(f"\nTreated universities:")
for _, r in treated_df.iterrows():
    print(f"  {r['university_name']} (rank {r['world_rank']:.0f}, n={r['n_obs']:.0f}, mean salary=${r['mean_salary']:,.0f})")

print(f"\nDonor universities:")
for _, r in donor_df.iterrows():
    print(f"  {r['university_name']} (rank {r['world_rank']:.0f}, n={r['n_obs']:.0f}, mean salary=${r['mean_salary']:,.0f})")

# ══════════════════════════════════════════════════════════════════════════════
# 3. COVARIATE MATRIX
# ══════════════════════════════════════════════════════════════════════════════
COVARIATE_COLS = [
    "mean_seniority",
    "pct_Bachelor", "pct_Master", "pct_MBA", "pct_Doctor",
    "pct_Engineer", "pct_Sales", "pct_Admin", "pct_Marketing",
    "pct_Finance", "pct_Scientist", "pct_Operations",
]

# Standardise covariates (zero mean, unit variance) for distance calculation
all_unis = pd.concat([treated_df, donor_df], ignore_index=True)
cov_mean = all_unis[COVARIATE_COLS].mean()
cov_std = all_unis[COVARIATE_COLS].std().replace(0, 1)  # avoid /0

def standardise(df):
    return (df[COVARIATE_COLS] - cov_mean) / cov_std

# ══════════════════════════════════════════════════════════════════════════════
# 4. SYNTHETIC CONTROL — AGGREGATE TREATED
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SYNTHETIC CONTROL — AGGREGATE APPROACH")
print("=" * 60)
print("(Treating all top-100 universities as one unit)")

# Aggregate treated covariates (weighted by n_obs)
treated_weights = treated_df["n_obs"] / treated_df["n_obs"].sum()
treated_cov = (standardise(treated_df).T * treated_weights.values).sum(axis=1).values
treated_salary = (treated_df["mean_salary"] * treated_weights).sum()

# Donor covariate matrix
donor_cov = standardise(donor_df).values  # (J x K)
donor_salaries = donor_df["mean_salary"].values
J = len(donor_df)


def sc_objective(w, X_treated, X_donors):
    """Minimise squared distance between treated and synthetic covariates."""
    synthetic = X_donors.T @ w  # weighted average of donors
    return np.sum((X_treated - synthetic) ** 2)


# Constraints: weights sum to 1, non-negative
constraints = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}
bounds = [(0, 1)] * J
w0 = np.ones(J) / J  # equal initial weights

result = minimize(
    sc_objective, w0,
    args=(treated_cov, donor_cov),
    method="SLSQP",
    bounds=bounds,
    constraints=constraints,
    options={"maxiter": 1000, "ftol": 1e-12},
)

if not result.success:
    print(f"⚠ Optimisation warning: {result.message}")

w_star = result.x
synthetic_salary = w_star @ donor_salaries

print(f"\nTreated (aggregate top-100) mean salary:  ${treated_salary:,.0f}")
print(f"Synthetic control mean salary:             ${synthetic_salary:,.0f}")
print(f"Estimated causal effect:                   ${treated_salary - synthetic_salary:,.0f}")
print(f"Percentage premium:                        {(treated_salary - synthetic_salary) / synthetic_salary * 100:.1f}%")

# Show which donors got weight
print(f"\nSynthetic control weights (non-zero):")
for idx, w in enumerate(w_star):
    if w > 0.01:
        row = donor_df.iloc[idx]
        print(f"  {row['university_name']}: weight={w:.3f} (rank {row['world_rank']:.0f}, salary=${row['mean_salary']:,.0f})")

# ── Covariate balance ───────────────────────────────────────────────────────
print(f"\nCovariate balance (treated vs synthetic):")
synthetic_cov = donor_cov.T @ w_star
print(f"  {'Covariate':<20} {'Treated':>10} {'Synthetic':>10} {'Diff':>10}")
for i, col in enumerate(COVARIATE_COLS):
    # Show in original scale
    t_val = treated_cov[i] * cov_std.iloc[i] + cov_mean.iloc[i]
    s_val = synthetic_cov[i] * cov_std.iloc[i] + cov_mean.iloc[i]
    print(f"  {col:<20} {t_val:>10.3f} {s_val:>10.3f} {t_val - s_val:>10.3f}")

# ══════════════════════════════════════════════════════════════════════════════
# 5. PLACEBO TESTS (INFERENCE)
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("PLACEBO TESTS")
print("=" * 60)
print("(Apply synthetic control to each donor university as if it were treated)")

placebo_effects = []

for j in range(J):
    # Treat donor j as the "treated" unit; remaining donors are the pool
    placebo_treated_cov = donor_cov[j]
    placebo_treated_salary = donor_salaries[j]

    # Remaining donors (exclude j)
    mask = np.ones(J, dtype=bool)
    mask[j] = False
    placebo_donor_cov = donor_cov[mask]
    placebo_donor_salaries = donor_salaries[mask]
    J_placebo = J - 1

    constraints_p = {"type": "eq", "fun": lambda w: np.sum(w) - 1.0}
    bounds_p = [(0, 1)] * J_placebo
    w0_p = np.ones(J_placebo) / J_placebo

    res_p = minimize(
        sc_objective, w0_p,
        args=(placebo_treated_cov, placebo_donor_cov),
        method="SLSQP",
        bounds=bounds_p,
        constraints=constraints_p,
        options={"maxiter": 1000, "ftol": 1e-12},
    )

    if res_p.success:
        synth_sal = res_p.x @ placebo_donor_salaries
        effect = placebo_treated_salary - synth_sal
        placebo_effects.append(effect)

true_effect = treated_salary - synthetic_salary
placebo_effects = np.array(placebo_effects)

# p-value: proportion of placebo effects as large as the true effect
n_as_large = np.sum(np.abs(placebo_effects) >= np.abs(true_effect))
p_value = (n_as_large + 1) / (len(placebo_effects) + 1)  # add 1 for the treated unit

print(f"\nTrue treatment effect:   ${true_effect:,.0f}")
print(f"Placebo effects:")
print(f"  Mean:                  ${placebo_effects.mean():,.0f}")
print(f"  Std:                   ${placebo_effects.std():,.0f}")
print(f"  Min:                   ${placebo_effects.min():,.0f}")
print(f"  Max:                   ${placebo_effects.max():,.0f}")
print(f"  Median:                ${np.median(placebo_effects):,.0f}")
print(f"\nPlacebo effects as large or larger than true effect: {n_as_large}/{len(placebo_effects)}")
print(f"Pseudo p-value: {p_value:.3f}")

if p_value < 0.05:
    print("→ SIGNIFICANT at 5% level — university rank has a causal effect on salary")
elif p_value < 0.10:
    print("→ MARGINALLY SIGNIFICANT at 10% level")
else:
    print("→ NOT SIGNIFICANT — cannot reject that the effect is due to chance")

# ══════════════════════════════════════════════════════════════════════════════
# 6. PER-UNIVERSITY SYNTHETIC CONTROL (if multiple treated)
# ══════════════════════════════════════════════════════════════════════════════
if len(treated_df) > 1:
    print("\n" + "=" * 60)
    print("PER-UNIVERSITY SYNTHETIC CONTROL")
    print("=" * 60)

    for idx, treated_row in treated_df.iterrows():
        uni_name = treated_row["university_name"]
        t_cov = standardise(treated_df.loc[[idx]]).values.flatten()
        t_salary = treated_row["mean_salary"]

        res_u = minimize(
            sc_objective, w0,
            args=(t_cov, donor_cov),
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 1000, "ftol": 1e-12},
        )

        if res_u.success:
            synth_sal = res_u.x @ donor_salaries
            effect = t_salary - synth_sal
            print(f"\n  {uni_name} (rank {treated_row['world_rank']:.0f}):")
            print(f"    Actual salary:    ${t_salary:,.0f}")
            print(f"    Synthetic salary: ${synth_sal:,.0f}")
            print(f"    Effect:           ${effect:,.0f} ({effect/synth_sal*100:.1f}%)")

            # Show top weights
            top_w = np.argsort(res_u.x)[::-1][:3]
            print(f"    Top donors: ", end="")
            parts = []
            for wi in top_w:
                if res_u.x[wi] > 0.01:
                    parts.append(f"{donor_df.iloc[wi]['university_name']} ({res_u.x[wi]:.2f})")
            print(", ".join(parts) if parts else "no dominant donor")

# ══════════════════════════════════════════════════════════════════════════════
# 7. SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"""
Research question: Does attending a top-100 ranked university
cause higher salary outcomes?

Method: Imbens synthetic control (Abadie et al. 2010)
- Matched on: seniority, degree mix, job category mix
- Unit: university (aggregated from individual data)
- Treatment: {len(treated_df)} universities ranked in top 100
- Donor pool: {len(donor_df)} universities ranked 101+

Result:
  Treated salary:    ${treated_salary:,.0f}
  Synthetic salary:  ${synthetic_salary:,.0f}
  Causal estimate:   ${treated_salary - synthetic_salary:,.0f} ({(treated_salary - synthetic_salary) / synthetic_salary * 100:.1f}%)
  p-value:           {p_value:.3f}

Caveat: Based on {analysis.shape[0]} observations from random_sample_1000.
Full positions dataset would substantially increase power.
""")

print("Done.")
