# School-to-Employer Networks Analysis — Lab Journal

## Project Overview
<!-- Describe the project goal here -->

## Storage Location (SSH)
- Server: `rdp-ssh.arc.ucl.ac.uk`
- Username: `rmhztos`
- Data path: `/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/`
- Working directory: `~/school-to-employer-networks-analysis/`

---

## Steps

### Step 1 — Environment Setup
- Date: 17 March 2026
- Created project folder structure on SSH server (`rdp-ssh.arc.ucl.ac.uk`)
- Command run:
  ```bash
  mkdir -p ~/school-to-employer-networks-analysis/{raw,interim,processed,logs,figures,scripts}
  ```
- Outcome: Folders created — raw, interim, processed, logs, figures, scripts

### Step 2 — Data Discovery
- Date: 17 March 2026
- Storage area: `/rdss/rd01/ritd-ag-project-rd01mo-pengz35/data/`
- Three datasets found:
  - `positions/` (positions1–4): hundreds of `position_XXXX_part_00.csv` files, many up to 4.7GB each — very large, many empty (0 bytes)
  - `random_sample_1000/`: 101 CSV files (~4–6MB each, ~1GB total) — pre-prepared random sample, best for initial exploration
  - `user_education/`: one real file `user_education_0000_part_00.csv` (470MB), rest are empty
- Decision: Start exploration with `random_sample_1000` (manageable size) and `user_education_0000_part_00.csv`
- Note: Do NOT copy full positions dataset — too large. Work directly from source path.

### Step 3 — Data Ingestion
- Date: 17 March 2026
- Decision: Work directly from source paths (files too large to copy)
- Join key between both datasets: `user_id`

**Positions dataset columns** (`random_sample_1000/`):
user_id, position_id, company_raw, company_linkedin_url, company_cleaned,
company_priname, location_raw, region, country, state, msa, startdate, enddate,
jobtitle_raw, mapped_role, job_category, role_k50, role_k150, role_k300,
role_k500, role_k1000, remote_suitability, weight, description,
start_mean_sampled_salary, end_mean_sampled_salary, seniority, salary, rn,
rcid, company_name, ultimate_parent_rcid, ultimate_parent_company_name

**Education dataset columns** (`user_education/`):
user_id, school, university_name, rn, startdate, enddate, degree, field,
degree_raw, field_raw, world_rank, us_rank, university_country, university_location

**Positions — all 101 sample files combined:**
- Shape: 869,195 rows × 33 columns
- Unique users: 857,329
- Unique companies: 288,305
- Unique job categories: 28
- Top countries: empty (432,567), United States (95,519), United Kingdom (40,188), Brazil (28,113), India (24,394)
- Top job categories: Sales (208,854), Engineer (197,134), Admin (157,292), Marketing (90,293), Finance (82,042)

**Education — user_education_0000_part_00.csv:**
- Shape: 3,096,577 rows × 14 columns
- `degree` has 1,222,994 nulls (~39% missing)
- `field` has 1,222,994 nulls (~39% missing)
- `university_country` has 1,651,256 nulls (~53% missing)
- Top universities: The Open University, University College Cork, Aston University
- Top degrees: empty, Bachelor, Master, High School, MBA
- Top fields: empty, Engineering, Business, Law, IT, Education

**Join issue:** `user_id` is `int64` in education but `object` (string) in positions — fixed by casting both to string before merge.

- Outcome: Schema understood. Ready to explore and join datasets via user_id.

### Step 4 — Data Cleaning
- Date: 18 March 2026
- Scripts: `02_clean.py` (cleaning + save), `03_sanity_check.py` (validation)

**Issues identified from profiling (Steps 2–3):**

| Column | Dataset | Issue | Rows Affected |
|---|---|---|---|
| `country` | Positions | Literal string `"empty"` instead of NaN | 432,567 (~50%) |
| `degree` | Education | Literal string `"empty"` instead of NaN | 1,101,405 (~36%) |
| `field` | Education | Literal string `"empty"` instead of NaN | 1,357,549 (~44%) |
| `startdate` | Positions | String object — needs datetime parse | all rows |
| `enddate` | Positions | String object — needs datetime parse; NaN = current role | 1,727 nulls |
| `startdate` | Education | String object — needs datetime parse | 448,465 nulls |
| `enddate` | Education | String object — needs datetime parse | 534,030 nulls |
| `company_name` | Positions | Null — no usable employer | 298 rows |
| `description` | Positions | Malformed quoting + 54% null — exclude from analysis | 4,183 nulls |
| `user_id` | Both | Type mismatch: `int64` (education) vs `object` (positions) | all rows |
| `job_category` | Positions | Data errors: `"1"`, `"4"`, `"Axia Value Chain LLC"` leaked into column | 6 rows |
| `position_id` | Positions | Possible duplicates across sharded files | TBD |
| `startdate` | Positions | Dates as early as 1955 — possible outliers | TBD |

**Decisions made:**
- UK filter: deferred — clean all data first, filter downstream
- Open roles (`enddate` NaN): keep — these represent current employment
- Employer unit: keep both `company_name` (subsidiary) and `ultimate_parent_company_name` (parent group)
- Education rows missing degree+field: flag only, never drop (they still have `university_name`)
- Drop positions only if ALL of `company_name`, `company_raw`, `company_cleaned` are null (not just `company_name`)

**Cleaning steps implemented in `02_clean.py`:**
1. Cast `user_id` to string in both datasets
2. Replace `"empty"` strings with `NaN` across all columns
3. Drop the `description` column (54% null + malformed quoting)
4. Deduplicate positions on `user_id` + `position_id` + `company_name` (guards against shard overlaps while keeping same position at different companies)
5. Drop positions rows where `company_name`, `company_raw`, AND `company_cleaned` are all null
6. Parse `startdate` / `enddate` to datetime in both datasets (prints silent-failure counts)
7. Flag education rows where both `degree` and `field` are NaN (`has_degree_info` column — no rows dropped)
8. Save cleaned DataFrames to `cleaned/` as parquet for fast reload

**Validation in `03_sanity_check.py`:**
- Row counts before/after cleaning
- No `"empty"` strings remain
- `user_id` dtype is consistent across both datasets
- `description` column removed
- Date columns are datetime dtype
- No all-null employer rows
- `has_degree_info` flag present
- Full dtype summary of saved parquet files

**Execution results (18 March 2026):**

| Metric | Positions | Education |
|---|---|---|
| Raw rows | 869,195 | 3,096,577 |
| Cleaned rows | 866,643 | 3,096,577 |
| Columns (after clean) | 32 | 15 |
| `"empty"` strings replaced | 2,525,761 | 2,458,955 |
| Duplicates removed | 7 | — |
| No-employer rows dropped | 2,545 | — |
| Numeric columns recast | 9 | — |
| `startdate` silent parse failures | 135 | 0 |
| `enddate` silent parse failures | 71 | 0 |
| Rows flagged `has_degree_info=False` | — | 2,197,738 |
| Parquet file size | 134.7 MB | 112.0 MB |
| Saved to | `cleaned/positions_clean.parquet` | `cleaned/education_clean.parquet` |

**Notes:**
- 2,545 rows had no employer identity at all (more than the 298 with just `company_name` null — `company_raw` and `company_cleaned` caught the rest)
- 206 silent date parse failures total — negligible vs 869k rows; likely malformed date strings
- Education had zero parse failures — cleaner date formatting in that dataset

### Step 5 — Analysis
- Date: 18 March 2026
- Script: `04_analyse.py` (descriptive + synthetic control prep)

**Research questions:**
1. Does university ranking affect salary outcomes?
2. What degrees earn the highest salaries — and does university rank moderate that?
3. Data linkage quality between salary and university
4. Imbens synthetic control: do top-ranked universities still yield higher salaries after matching on observables?

**Analysis design (`04_analyse.py`):**
1. Merge cleaned education + positions on `user_id` (inner join)
2. Filter to rows with both `salary` and `world_rank`
3. Data linkage quality report (match rates)
4. Salary by university rank quartile (Q1=top → Q4=bottom)
5. Salary by degree (Bachelor, Master, MBA, etc.)
6. Interaction: degree × rank quartile → salary (key question: same degree, different rank = different salary?)
7. Top 20 universities by median salary
8. Salary by seniority × rank quartile
9. Synthetic control preparation:
   - Treatment: graduates from top-100 world-ranked universities
   - Control pool: graduates from universities ranked 101+
   - Covariates to match on: degree mix, field mix, job_category mix, seniority
   - Outcome: salary
   - Covariate balance table (treatment vs control)

**Preliminary findings (from 01_explore.py on raw data):**
- Merged shape: 9,197 rows × 14 columns
- Unique users in both datasets: 8,196

**Results from 04_analyse.py (18 March 2026):**

*Data linkage:*
- Position users: 854,786 | Education users: 2,900,907
- Matched (inner join): 8,196 (1.0% of position users)
- With both salary + world_rank: 2,617 observations, 2,165 unique users
- Match rate is low — the random_sample_1000 covers a small subset; full positions dataset would yield more matches

*Salary by university rank quartile:*

| Quartile | Count | Mean Salary | Median Salary |
|---|---|---|---|
| Q1 (top) | 826 | $38,876 | $32,762 |
| Q2 | 673 | $35,932 | $27,994 |
| Q3 | 662 | $38,032 | $31,158 |
| Q4 (bottom) | 456 | $31,571 | $24,027 |

- **Q1 vs Q4 gap: $7,305 (23.1%)** — top-ranked graduates earn ~23% more

*Salary by degree:*

| Degree | Count | Mean | Median |
|---|---|---|---|
| MBA | 57 | $44,930 | $38,781 |
| Bachelor | 543 | $36,990 | $31,420 |
| Doctor | 33 | $35,165 | $26,701 |
| Master | 193 | $34,148 | $27,930 |

- MBA holders earn the most; Master's is below Bachelor's (possibly due to career stage or field differences)

*Key finding — Degree × Rank interaction (same degree, top vs bottom university):*

| Degree | Q1 (top) | Q4 (bottom) | Gap | Gap % |
|---|---|---|---|---|
| Bachelor | $39,429 | $34,055 | +$5,374 | +16% |
| Doctor | $33,997 | $21,000 | +$12,997 | +62% |
| MBA | $41,888 | $24,815 | +$17,073 | +69% |
| Master | $31,933 | $35,864 | -$3,931 | -11% |

- **MBA and Doctor degrees show the strongest university rank premium** (69% and 62%)
- **Bachelor's shows a moderate 16% premium** for top-ranked universities
- **Master's is an anomaly** — bottom-ranked universities yield higher salaries (possibly selection bias: older/experienced professionals getting Master's at accessible institutions)

*Seniority × Rank:*
- Rank premium is strongest at seniority level 1 (entry-level): Q1=$27,917 vs Q4=$21,297 (+31%)
- Premium narrows at mid-career (seniority 3): Q1=$36,452 vs Q4=$37,915 (slightly negative)
- Premium returns at senior levels (seniority 4-5): Q1=$46,986 vs Q4=$35,974 (+31%)

*Synthetic control preparation:*
- Treatment group (rank ≤ 100): 266 observations
- Control pool (rank > 100): 2,351 observations
- Raw treated vs control salary gap: $3,798
- **Covariate imbalance detected**: treated group has more Master's (43.7% vs 20.8%), fewer Bachelor's (54% vs 66.7%), lower mean seniority (1.99 vs 2.36)
- This imbalance means the raw $3,798 gap is NOT causal — synthetic control matching needed

*Synthetic control method (`05_synthetic_control.py`):*
- Imbens/Abadie et al. (2010) synthetic control method
- Aggregates individual data to university level (min 10 observations per university)
- Covariates matched on: mean seniority, degree mix (Bachelor/Master/MBA/Doctor %), job category mix (7 categories)
- Treatment: universities ranked in world top 100
- Donor pool: universities ranked 101+
- Weights: non-negative, sum to 1, found via SLSQP constrained optimisation (scipy)
- Inference: placebo tests — apply synthetic control to each donor uni and compare effect sizes
- p-value: proportion of placebo effects as large as the true effect
- Also runs per-university synthetic control for individual treated universities
- No external dependencies beyond pandas, numpy, scipy

*Synthetic control results (18 March 2026):*

| Metric | Value |
|---|---|
| Universities with ≥10 observations | 20 |
| Treated (rank ≤ 100) | 2 (Chicago rank 10, Groningen rank 80) |
| Donor pool (rank > 100) | 18 |
| Treated aggregate mean salary | $42,280 |
| Synthetic control mean salary | $32,567 |
| **Estimated causal effect** | **$9,713 (29.8%)** |
| **Pseudo p-value (placebo tests)** | **0.158 — NOT significant** |

*Per-university results:*

| University | Rank | Actual Salary | Synthetic Salary | Effect | Effect % |
|---|---|---|---|---|---|
| University of Chicago | 10 | $50,165 | $39,940 | +$10,225 | +25.6% |
| University of Groningen | 80 | $40,676 | $32,116 | +$8,561 | +26.7% |

*Synthetic control composition:*
- Ghent Belgium (43.1%), La Laguna (29.0%), Kyiv (8.0%), East Anglia (6.2%), Aston (5.0%), A Coruña (4.7%), Lille (4.1%)

*Covariate balance (treated vs synthetic):*
- Seniority: 1.75 vs 1.98 (diff -0.23) — slight imbalance
- Degree mix: well balanced (Bachelor 65% vs 66%, Master 33% vs 30%)
- Job category: well balanced (largest diff: Operations 7% vs 3%)

*Interpretation:*
- Point estimate suggests a ~30% salary premium for top-100 university graduates
- Both Chicago (+25.6%) and Groningen (+26.7%) show consistent premiums
- **BUT p=0.158 means we cannot rule out chance** — 2 of 18 placebo effects were as large or larger
- Key limitation: only 2 treated universities met the 10-observation threshold in this sample
- The full positions dataset (hundreds of GBs) would yield far more matched universities and statistical power

**Top university → job category flows:**
| University | Job Category | Count |
|---|---|---|
| The Open University | Engineer | 130 |
| The Open University | Admin | 122 |
| Universidad de La Laguna | Scientist | 97 |
| The Open University | Sales | 88 |
| University College Cork | Engineer | 75 |
| University of Ghent Belgium | Admin | 63 |
| The Open University | Scientist | 60 |
| University of Aston in Birmingham | Admin | 54 |
| Marketing Week Mini MBA with Mark Ritson | Marketing | 50 |

**Top degree → job category flows:**
| Degree | Job Category | Count |
|---|---|---|
| empty | Engineer | 572 |
| empty | Sales | 529 |
| empty | Admin | 512 |
| Bachelor | Engineer | 315 |
| Bachelor | Sales | 255 |
| Bachelor | Admin | 228 |
| High School | Sales | 83 |
| Master | Engineer | 82 |

**Key observations:**
- The Open University is the dominant school → employer pipeline in this sample
- Bachelor's degrees lead to Engineering and Sales most frequently
- ~39% of degree fields are missing ("empty") — limits field-level analysis
- Only 8,196 of 857,329 position users matched education records in this sample — suggests the sample covers a small subset of the full dataset

### Step 6 — Export
<!-- Date: -->
<!-- Output files: -->
<!-- Location: -->

---

## Issues & Notes
- **Malformed CSV quoting in positions files**: The `description` column contains unescaped double quotes inside quoted strings, causing parse errors. Workaround: `on_bad_lines='skip'` used during exploration. A small number of rows are lost. For production analysis, pre-processing with `sed` or a custom parser will be needed to fix the raw files.
- **`sed` fix only patched first `read_csv` call**: The `concat` loop still used old parameters. Fixed by running a second `sed` command targeting `pd.read_csv(f, low_memory=False)`.
- **Single file preview successful** (17 March 2026): Shape 7677 rows × 33 columns. Key findings:
  - Majority of positions are UK-based (5184 / 7677)
  - 1647 rows have `country = "empty"` — needs cleaning
  - Top job categories: Engineer, Sales, Finance, Marketing, Admin
  - `description` has 4183 nulls (~54% missing) — not reliable for analysis
  - `enddate` has 1727 nulls — many open/current positions
  - `startdate` ranges from 1955 to 2023
  - Seniority levels 1–7 present; most are level 2 and 1
  - `company_linkedin_url` has 1266 nulls — not useful as join key

---

## Data Security Reminders
- Raw data stays on the SSH server — never commit it to GitHub
- Only scripts, comments, and non-sensitive notes go in this repo
- No personal/identifiable data in any committed file
