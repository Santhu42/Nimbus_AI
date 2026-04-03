# NimbusAI Customer Retention Analysis | Setup & Execution Guide

This project provides a comprehensive data-driven investigation into customer churn at NimbusAI. It processes raw PostgreSQL and MongoDB datasets into actionable insights for the VP of Product and Head of Customer Success.

---

## 🛠️ 1. Database Access & Local Setup

Before running the analysis, ensure the raw data is imported into your local PostgreSQL and MongoDB instances.

### PostgreSQL (Nimbus Core)
```powershell
# 1. Create the database
psql -U postgres -c "CREATE DATABASE nimbus_db;"

# 2. Import the SQL dump
psql -U postgres -d nimbus_db -f "nimbus_core.sql"
```

### MongoDB (Nimbus Events)

**Option A — Using `mongosh` (if installed):**
```powershell
mongosh "mongodb://localhost:27017/nimbus_db" "nimbus_events.js"
```

**Option B — Using Node.js (recommended if `mongosh` is not available):**
```powershell
# 1. Install Mongoose
npm install mongoose

# 2. Run the import script
node import_mongo_node.js
```
> This will parse `nimbus_events.js` and insert all documents into `nimbus_db` with three collections: `user_activity_logs`, `nps_survey_responses`, and `onboarding_events`.

---

## 🔧 2. Troubleshooting & Fixes

### ⚠️ PostgreSQL: `syntax error at or near "CREATE"`
The raw `nimbus_core.sql` file may contain a missing semicolon on line **11630**. This causes the import to fail at the `billing_invoices` table.

**Fix:** A semicolon `;` has been added to the end of the subscription insert block (Line 11630).

### ⚠️ MongoDB Compass: `load is not implemented`
The Compass embedded shell does not support the `load()` command. Use the **Node.js import script** (`node import_mongo_node.js`) described above instead.

---

## 🚀 3. Quick Start (Standard Execution)

Follow these steps in order to process the raw data and generate the analysis:

### Step 1: Install Dependencies
Ensure you have Python 3.10+ installed. Install the required data science libraries:
```powershell
pip install pandas numpy scipy pymongo
npm install mongoose
```

### Step 2: Data Extraction & Cleaning (ETL)
Run the core processing script to parse the SQL dump and MongoDB JS export into normalized CSVs.
```powershell
python data_processing.py
```
*Output: Cleaned datasets in `/processed_data/`*

### Step 3: Run Data Wrangling & Analysis (Task 3)
Merge SQL + MongoDB data, run hypothesis tests, and generate customer segments.
```powershell
python task3_analysis.py
```
*Output: `processed_data/master_customer_360.csv` and `processed_data/customer_segments.csv`*

### Step 4: Open Power BI Dashboard
```powershell
Start-Process "Roado Project.pbix"
```

---

## 📊 4. Testing Queries

### PostgreSQL — Testing in pgAdmin
1. Open **pgAdmin 4** and connect to `nimbus_db`.
2. Right-click the database → **Query Tool**.
3. Open `nimbus_core_queries.sql` (click the folder icon).
4. **Select one query at a time** and press **F5** (or the Play button) to execute.
5. Results will appear in the **Data Output** panel below.

| Query | Description |
|-------|-------------|
| Query 1 | Monthly Churn Rate by Plan Tier |
| Query 2 | Customer Lifetime Value (LTV) by Company Size |
| Query 3 | Support Ticket Correlation (Leading Indicator) |

### MongoDB — Testing in Compass
1. Open **MongoDB Compass** and connect to `localhost:27017`.
2. Navigate to `nimbus_db` → select a collection (e.g., `user_activity_logs`).
3. Click the **"Aggregations"** tab.
4. Add pipeline stages one at a time using the dropdown.

#### Example: Feature Adoption Ranking
- Collection: `user_activity_logs`
- Stage 1 — `$match`:
  ```json
  { "event_type": "feature_click" }
  ```
- Stage 2 — `$group`:
  ```json
  { "_id": "$feature", "usage_count": { "$sum": 1 } }
  ```
- Stage 3 — `$sort`:
  ```json
  { "usage_count": -1 }
  ```

#### Example: NPS Sentiment (on `nps_survey_responses`)
- Stage 1 — `$project`:
  ```json
  { "category": { "$switch": { "branches": [{ "case": { "$gte": ["$nps_score", 9] }, "then": "Promoter" }, { "case": { "$gte": ["$nps_score", 7] }, "then": "Passive" }], "default": "Detractor" } } }
  ```
- Stage 2 — `$group`:
  ```json
  { "_id": "$category", "count": { "$sum": 1 } }
  ```

> **Tip:** Compass shows a live preview of results after each stage. You can export results to JSON/CSV.

---

## 📈 5. Task 3 — Data Wrangling & Statistical Analysis

Script: `task3_analysis.py`

### Merge & Clean Summary

| Dataset | Before | After | Cleaning Applied |
|---------|--------|-------|------------------|
| customers | 1,204 | 1,204 | Type coercion, boolean mapping, derived `is_churned` + `tenure_days` |
| subscriptions | 1,840 | 1,840 | Date parsing, numeric coercion |
| team_members | 8,500 | 8,500 | Boolean mapping, deduplication check |
| user_activity_logs | 51,485 | 50,000 | Field normalization (`customerId`→`customer_id`), UTC timezone normalization, outlier capping at 3×P99, 1,485 duplicates removed |
| nps_survey_responses | 3,000 | 3,000 | Type coercion, UTC normalization |
| onboarding_events | 8,000 | 8,000 | Field normalization (`customerId`/`memberId`), UTC normalization |
| **Master Merged** | — | **1,204 × 34** | Left joins on `customer_id` across all sources |

### Hypothesis Test

| Item | Value |
|------|-------|
| H₀ | No difference in activity events between churned and active customers |
| H₁ | Active customers have more events than churned |
| Test | Mann-Whitney U (non-parametric, skewed data) |
| α | 0.05 |
| p-value (one-sided) | **0.044** |
| Result | **Reject H₀** — higher engagement is associated with retention |
| Effect Size | r = -0.068 (negligible) |

### Customer Segmentation (RFV)

| Segment | Count | Avg Events | Avg MRR | Churn Rate | Strategy |
|---------|-------|------------|---------|------------|----------|
| 🟢 Champions | 209 | 46.4 | $53.64 | 24% | VIP support, referral incentives |
| 🔵 Loyal | 156 | 45.3 | $9.22 | 19% | Upsell to higher plans |
| 🟡 New/Promising | 258 | 35.6 | $31.90 | 27% | Onboarding campaigns, activation |
| 🟠 At Risk | 254 | 45.2 | $26.83 | 17% | Win-back campaigns, outreach |
| 🔴 High-Value Sleepers | 157 | 35.4 | $53.78 | 17% | Proactive CSM, ROI demos |
| ⚪ Dormant/Lost | 140 | 35.0 | $4.37 | 29% | Re-engagement or accept churn |

---

## 🎨 6. Task 4 — Power BI Dashboard

### 📥 Download & Open
1. **Download** the Power BI file: [Roado Project.pbix](https://drive.google.com/file/d/1eNvTYbFNoYN30MoyGfPr05jYvlVDf26x/view?usp=sharing)
2. Ensure **Power BI Desktop** is installed on your machine ([free download](https://powerbi.microsoft.com/desktop/))
3. Open the downloaded `.pbix` file — the dashboard will load with all visuals, filters, and data pre-configured

### KPI Summary

| Metric | Value |
|--------|-------|
| Total Customers | 1,204 |
| Churn Rate | 22% |
| Total MRR | $37.11K |
| Avg NPS | 6.86 |

### Power BI Setup (to recreate)
1. Open **Power BI Desktop** → **Get Data** → **Text/CSV**
2. Import from `processed_data/`: `master_customer_360.csv`, `customer_segments.csv`, `subscriptions.csv`, `user_activity_logs.csv`, `onboarding_events.csv`
3. **Model View** → drag `customer_id` to create relationships (master = "1" side, others = "*" side)
4. **Report View** → build visuals using the fields from each table
5. Add **Slicers** for `plan_tier` and `industry` as interactive filters

### 8 Visualizations

| Visual | Type | Key Fields | Source |
|--------|------|-----------|--------|
| Churn by Plan Tier | Clustered Bar + Line | plan_tier, customer_id (Count), is_churned (Avg) | SQL |
| Monthly Churn Trend | Area Chart | churned_at (Month), customer_id (Count) | SQL |
| Engagement vs Churn | Scatter (bubble = MRR) | total_events, avg_session_sec, mrr_usd, is_churned | **SQL + MongoDB** |
| Churned Customers | Bar Chart | churned_at (Month), is_churned (Sum) | SQL |
| Churn by Industry | Horizontal Bar | industry, is_churned (Avg) | SQL |
| Onboarding Funnel | Funnel | step, member_id (Distinct Count) | MongoDB |
| NPS Active vs Churned | Clustered Bar | latest_nps, customer_id (Count), is_churned | SQL + MongoDB |
| Customer Segments | Treemap | segment, customer_id (Count) | SQL + MongoDB |

**2 Interactive Filters (Slicers):** Plan Tier, Industry

### 3 Actionable Recommendations
1. Launch a "High-Value Sleeper" Re-Activation Program (est. $1,340/mo MRR saved)
2. Fix the onboarding funnel at the "Invited Teammate" step (+25% invite rate target)
3. Create industry-specific retention playbooks for high-churn verticals

---

## 📂 7. Project Directory Structure
```text
roado/
├── nimbus_core.sql              # Raw PostgreSQL dump
├── nimbus_events.js             # Raw MongoDB export
├── import_mongo_node.js         # Node.js MongoDB import helper
│
├── nimbus_core_queries.sql      # Task 1: SQL queries (3 analytical queries)
├── nimbus_events_aggregations.js # Task 2: MongoDB aggregation pipelines (4 queries)
├── task3_analysis.py            # Task 3: Merge, Hypothesis Test, Segmentation
├── Roado Project.pbix           # Task 4: Power BI dashboard
│
├── data_processing.py           # ETL Engine (dependency of task3)
├── package.json                 # Node.js deps (mongoose)
│
├── processed_data/              # Generated CSVs
│   ├── master_customer_360.csv  # Unified customer table (1,204 × 34)
│   ├── customer_segments.csv    # RFV segmentation with scores
│   ├── customers.csv            # Cleaned SQL customers
│   ├── subscriptions.csv        # Cleaned SQL subscriptions
│   ├── plans.csv                # Plan tier reference (8 plans)
│   ├── team_members.csv         # Cleaned SQL team members
│   ├── user_activity_logs.csv   # Cleaned MongoDB activity (50K events)
│   ├── nps_survey_responses.csv # Cleaned MongoDB NPS (3K surveys)
│   └── onboarding_events.csv   # Cleaned MongoDB onboarding (8K events)
│
└── README.md                    # This file
```
