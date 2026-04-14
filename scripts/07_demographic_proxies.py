"""
07_demographic_proxies.py
-------------------------
Explore potential demographic (gender/race) proxy indicators in the data.
No direct demographic columns exist — this script investigates indirect signals.

Usage (on SSH server):
    python scripts/07_demographic_proxies.py
"""

import pandas as pd

CLEANED_DIR = "cleaned/"

print("Loading cleaned data...")
pos = pd.read_parquet(f"{CLEANED_DIR}/positions_clean.parquet")
edu = pd.read_parquet(f"{CLEANED_DIR}/education_clean.parquet")

# ── 1. Geographic proxies (country, state, MSA) ────────────────────────────
print("=" * 60)
print("1. GEOGRAPHIC PROXIES")
print("=" * 60)

print("\n--- Country distribution (top 20) ---")
print(pos["country"].value_counts(dropna=False).head(20).to_string())

print("\n--- US States (top 20) ---")
us = pos[pos["country"] == "United States"]
print(f"US positions: {len(us)}")
print(us["state"].value_counts(dropna=False).head(20).to_string())

print("\n--- MSA (Metropolitan Statistical Areas, top 20) ---")
print(us["msa"].value_counts(dropna=False).head(20).to_string())
print("\nNote: US MSAs can be linked to Census demographic data (race/ethnicity")
print("composition) via ACS 5-year estimates.")

# ── 2. University-level demographic signals ─────────────────────────────────
print("\n" + "=" * 60)
print("2. UNIVERSITY-LEVEL DEMOGRAPHIC SIGNALS")
print("=" * 60)

# Known US HBCUs (Historically Black Colleges and Universities)
HBCUS = [
    "Howard University", "Spelman College", "Morehouse College",
    "Tuskegee University", "Hampton University", "Florida A&M University",
    "North Carolina A&T State University", "Morgan State University",
    "Jackson State University", "Prairie View A&M University",
    "Southern University", "Grambling State University",
    "Tennessee State University", "Alabama State University",
    "Delaware State University", "Fisk University", "Clark Atlanta University",
    "Bethune-Cookman University", "Xavier University of Louisiana",
    "Norfolk State University", "Alcorn State University",
    "Bowie State University", "Coppin State University",
    "Lincoln University", "Langston University", "Dillard University",
]

print("\n--- HBCU matches in education data ---")
hbcu_mask = edu["university_name"].str.strip().isin(HBCUS)
hbcu_rows = edu[hbcu_mask]
print(f"HBCU rows found: {len(hbcu_rows)}")
if len(hbcu_rows) > 0:
    print(hbcu_rows["university_name"].value_counts().to_string())

# Also do a fuzzy/partial match
print("\n--- Partial HBCU name matches (broader search) ---")
hbcu_keywords = ["HBCU", "A&M", "A & M", "Morehouse", "Spelman", "Howard Uni",
                  "Tuskegee", "Hampton Uni", "Florida A&M", "Bethune",
                  "Grambling", "Fisk Uni", "Xavier.*Louisiana"]
for kw in hbcu_keywords:
    matches = edu[edu["university_name"].str.contains(kw, case=False, na=False)]
    if len(matches) > 0:
        print(f"  '{kw}': {len(matches)} rows — {matches['university_name'].unique()[:3]}")

# Women's colleges
WOMENS_COLLEGES = [
    "Wellesley College", "Smith College", "Bryn Mawr College",
    "Barnard College", "Mount Holyoke College", "Scripps College",
    "Simmons University", "Mills College", "Sweet Briar College",
    "Agnes Scott College", "Hollins University", "Converse University",
]
print("\n--- Women's College matches ---")
wc_mask = edu["university_name"].str.strip().isin(WOMENS_COLLEGES)
wc_rows = edu[wc_mask]
print(f"Women's college rows found: {len(wc_rows)}")
if len(wc_rows) > 0:
    print(wc_rows["university_name"].value_counts().to_string())

# University country as ethnicity proxy
print("\n--- University country distribution (top 20) ---")
print(edu["university_country"].value_counts(dropna=False).head(20).to_string())

# ── 3. Field of study as demographic proxy ──────────────────────────────────
print("\n" + "=" * 60)
print("3. FIELD OF STUDY — KNOWN DEMOGRAPHIC SKEWS")
print("=" * 60)

print("\n--- Field distribution (top 30) ---")
print(edu["field"].value_counts(dropna=False).head(30).to_string())

# Fields with known gender skews (based on NCES/NSF data)
male_skew = ["Engineering", "Computer Science", "Physics", "Mathematics",
             "Information Technology", "IT", "Mechanical Engineering",
             "Electrical Engineering", "Civil Engineering"]
female_skew = ["Nursing", "Education", "Psychology", "Social Work",
               "English", "Communications", "Sociology", "Human Resources",
               "Public Health", "Art", "Literature"]

print("\n--- Male-skewed fields (nationally >60% male) ---")
for f in male_skew:
    count = edu[edu["field"].str.contains(f, case=False, na=False)].shape[0]
    if count > 0:
        print(f"  {f}: {count} rows")

print("\n--- Female-skewed fields (nationally >60% female) ---")
for f in female_skew:
    count = edu[edu["field"].str.contains(f, case=False, na=False)].shape[0]
    if count > 0:
        print(f"  {f}: {count} rows")

# ── 4. Job title gender signals ─────────────────────────────────────────────
print("\n" + "=" * 60)
print("4. JOB TITLE — GENDERED TERMS")
print("=" * 60)

gendered_terms = {
    "Chairman": "male-coded", "Chairwoman": "female-coded",
    "Salesman": "male-coded", "Saleswoman": "female-coded",
    "Businessman": "male-coded", "Businesswoman": "female-coded",
    "Foreman": "male-coded", "Forewoman": "female-coded",
    "Waitress": "female-coded", "Waiter": "male-coded",
    "Actress": "female-coded", "Actor": "male-coded",
    "Stewardess": "female-coded", "Midwife": "female-coded",
    "Nanny": "female-coded", "Maid": "female-coded",
}

for term, coding in gendered_terms.items():
    count = pos[pos["jobtitle_raw"].str.contains(rf"\b{term}\b", case=False, na=False)].shape[0]
    if count > 0:
        print(f"  {term} ({coding}): {count} rows")

# ── 5. Region as race/ethnicity proxy ───────────────────────────────────────
print("\n" + "=" * 60)
print("5. REGION DISTRIBUTION (race/ethnicity proxy via geography)")
print("=" * 60)

print("\n--- Position region distribution (top 20) ---")
print(pos["region"].value_counts(dropna=False).head(20).to_string())

# ── 6. Summary ──────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY — AVAILABLE DEMOGRAPHIC PROXIES")
print("=" * 60)
print("""
Direct indicators:       NONE (no name, gender, race, age columns)

Indirect proxies available:
  Geography  → country, state, MSA, region (linkable to Census ACS for
               area-level race/ethnicity composition)
  University → university_name (check for HBCUs, women's colleges)
               university_country (broad nationality signal)
  Field      → field of study (known gender skews from NCES/NSF data)
  Job title  → jobtitle_raw (rare gendered terms like Chairman/Chairwoman)

Recommended external linkages:
  1. US Census ACS 5-year → MSA/state-level race/ethnicity %
  2. IPEDS (nces.ed.gov)  → university-level gender/race enrollment %
  3. O*NET / BLS OES      → occupation-level gender/race %
  4. QS/THE rankings data → university country for international comparison

Limitations:
  - All proxies are ecological (area/institution level), NOT individual level
  - Ecological inference fallacy: area demographics ≠ individual demographics
  - These proxies are useful for descriptive/contextual analysis only
  - Not suitable for causal claims about race/gender effects on outcomes
""")
