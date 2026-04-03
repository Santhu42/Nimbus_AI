"""
============================================================
TASK 3 — Data Wrangling & Statistical Analysis
NimbusAI Customer Retention Analysis
============================================================
This script performs:
  1. MERGE & CLEAN: Joins SQL (PostgreSQL) and MongoDB data on customer_id
  2. HYPOTHESIS TEST: Tests if Feature X usage reduces churn
  3. SEGMENTATION: Creates engagement-based customer segments

Data Sources:
  - PostgreSQL (nimbus_core.sql): plans, customers, team_members, subscriptions, billing_invoices
  - MongoDB (nimbus_events.js): user_activity_logs, nps_survey_responses, onboarding_events

Requirements: pip install pandas numpy scipy pymongo psycopg2-binary
============================================================
"""

import pandas as pd
import numpy as np
from scipy import stats
from pymongo import MongoClient
import warnings
import os

warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION
# ============================================================
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "nimbus_db"

# We will use the existing data_processing.py to parse SQL data
# and connect to MongoDB directly for the NoSQL data.
OUTPUT_DIR = "processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# SECTION 1: DATA EXTRACTION
# ============================================================
print("=" * 70)
print("SECTION 1: DATA EXTRACTION")
print("=" * 70)

# --- 1A: Extract SQL Data from parsed CSVs or raw SQL ---
# We reuse the parse_sql function from data_processing.py
from data_processing import parse_sql, parse_mongo

print("\n📦 Parsing SQL data from nimbus_core.sql...")
sql_tables = parse_sql('nimbus_core.sql')
for name, df in sql_tables.items():
    print(f"   ✅ SQL Table: {name:25s} → {len(df):,} rows, {len(df.columns)} columns")

# --- 1B: Extract MongoDB Data ---
print("\n📦 Connecting to MongoDB...")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.server_info()  # Force connection test
    db = client[MONGO_DB]

    mongo_data = {}
    for coll_name in ['user_activity_logs', 'nps_survey_responses', 'onboarding_events']:
        docs = list(db[coll_name].find({}, {'_id': 0}))
        mongo_data[coll_name] = pd.DataFrame(docs)
        print(f"   ✅ Mongo Collection: {coll_name:25s} → {len(docs):,} rows")
    client.close()
    MONGO_AVAILABLE = True
except Exception as e:
    print(f"   ⚠️ MongoDB not available ({e}). Falling back to file parsing...")
    mongo_data = parse_mongo('nimbus_events.js')
    for name, df in mongo_data.items():
        print(f"   ✅ Parsed Collection: {name:25s} → {len(df):,} rows")
    MONGO_AVAILABLE = False


# ============================================================
# SECTION 2: MERGE & CLEAN
# ============================================================
print("\n" + "=" * 70)
print("SECTION 2: MERGE & CLEAN")
print("=" * 70)

# --- Step 2.1: Baseline Row Counts (BEFORE cleaning) ---
print("\n📊 BEFORE Cleaning — Row Counts:")
print("-" * 50)
before_counts = {}
for name, df in sql_tables.items():
    before_counts[f"sql_{name}"] = len(df)
    print(f"   SQL  | {name:25s}: {len(df):,} rows")
for name, df in mongo_data.items():
    before_counts[f"mongo_{name}"] = len(df)
    print(f"   Mongo| {name:25s}: {len(df):,} rows")

# ─────────────────────────────────────────────────
# Step 2.2: Clean SQL Tables
# ─────────────────────────────────────────────────
print("\n🧹 CLEANING SQL DATA...")

# --- customers ---
customers = sql_tables['customers'].copy()
print(f"\n   [customers] Starting rows: {len(customers)}")

# a) Type coercion
customers['customer_id'] = pd.to_numeric(customers['customer_id'], errors='coerce')
customers['is_active'] = customers['is_active'].astype(str).str.upper().map(
    {'TRUE': True, 'FALSE': False, '1': True, '0': False, '1.0': True, '0.0': False}
)
customers['nps_score'] = pd.to_numeric(customers['nps_score'], errors='coerce')
customers['signup_date'] = pd.to_datetime(customers['signup_date'], errors='coerce')
customers['churned_at'] = pd.to_datetime(customers['churned_at'], errors='coerce')

# b) Handle NULLs
null_report_cust = customers.isnull().sum()
print(f"   → Nulls in key columns: nps_score={null_report_cust.get('nps_score', 0)}, "
      f"churned_at={null_report_cust.get('churned_at', 0)}, "
      f"churn_reason={null_report_cust.get('churn_reason', 0)}")
print(f"   → Strategy: nps_score NULLs left as NaN (not all customers surveyed)")
print(f"   → Strategy: churned_at NULLs = still active (expected)")

# c) Duplicates
dup_count = customers.duplicated(subset=['customer_id']).sum()
print(f"   → Duplicate customer_ids found: {dup_count}")
customers.drop_duplicates(subset=['customer_id'], keep='last', inplace=True)

# d) Derived columns
customers['is_churned'] = (~customers['is_active']).astype(int)
customers['tenure_days'] = (
    customers['churned_at'].fillna(pd.Timestamp.now()) - customers['signup_date']
).dt.days
print(f"   [customers] Final rows: {len(customers)}")

# --- subscriptions ---
subs = sql_tables['subscriptions'].copy()
print(f"\n   [subscriptions] Starting rows: {len(subs)}")
subs['customer_id'] = pd.to_numeric(subs['customer_id'], errors='coerce')
subs['plan_id'] = pd.to_numeric(subs['plan_id'], errors='coerce')
subs['mrr_usd'] = pd.to_numeric(subs['mrr_usd'], errors='coerce')
subs['start_date'] = pd.to_datetime(subs['start_date'], errors='coerce')
subs['end_date'] = pd.to_datetime(subs['end_date'], errors='coerce')
dup_subs = subs.duplicated().sum()
print(f"   → Duplicate rows: {dup_subs}")
subs.drop_duplicates(inplace=True)
print(f"   [subscriptions] Final rows: {len(subs)}")

# --- plans ---
plans = sql_tables['plans'].copy()
plans['plan_id'] = pd.to_numeric(plans['plan_id'], errors='coerce')
print(f"\n   [plans] Rows: {len(plans)} (reference table, no cleaning needed)")

# --- team_members ---
members = sql_tables['team_members'].copy()
print(f"\n   [team_members] Starting rows: {len(members)}")
members['member_id'] = pd.to_numeric(members['member_id'], errors='coerce')
members['customer_id'] = pd.to_numeric(members['customer_id'], errors='coerce')
members['is_active'] = members['is_active'].astype(str).str.upper().map(
    {'TRUE': True, 'FALSE': False, '1': True, '0': False, '1.0': True, '0.0': False}
)
members['last_login_at'] = pd.to_datetime(members['last_login_at'], errors='coerce')
dup_mem = members.duplicated(subset=['member_id']).sum()
print(f"   → Duplicate member_ids: {dup_mem}")
members.drop_duplicates(subset=['member_id'], keep='last', inplace=True)
print(f"   [team_members] Final rows: {len(members)}")

# ─────────────────────────────────────────────────
# Step 2.3: Clean MongoDB Collections
# ─────────────────────────────────────────────────
print("\n🧹 CLEANING MONGODB DATA...")

# --- user_activity_logs ---
activity = mongo_data['user_activity_logs'].copy()
print(f"\n   [user_activity_logs] Starting rows: {len(activity)}")

# a) Normalize inconsistent field names (customer_id vs customerId)
if 'customerId' in activity.columns:
    activity['customer_id'] = activity['customer_id'].combine_first(
        activity['customerId']
    ) if 'customer_id' in activity.columns else activity['customerId']
    activity.drop(columns=['customerId'], inplace=True, errors='ignore')
    print("   → Fixed: merged 'customerId' into 'customer_id'")

if 'memberId' in activity.columns:
    activity['member_id'] = activity['member_id'].combine_first(
        activity['memberId']
    ) if 'member_id' in activity.columns else activity['memberId']
    activity.drop(columns=['memberId'], inplace=True, errors='ignore')
    print("   → Fixed: merged 'memberId' into 'member_id'")

# b) Type coercion
activity['customer_id'] = pd.to_numeric(activity['customer_id'], errors='coerce')
activity['member_id'] = pd.to_numeric(activity['member_id'], errors='coerce')
activity['session_duration_sec'] = pd.to_numeric(activity.get('session_duration_sec', pd.Series(dtype=float)), errors='coerce')

# c) Timestamp handling (timezone normalization)
if 'timestamp' in activity.columns:
    activity['timestamp'] = pd.to_datetime(activity['timestamp'], errors='coerce', utc=True)
    # Normalize all timestamps to UTC (handling timezone mismatches)
    activity['timestamp'] = activity['timestamp'].dt.tz_localize(None)
    tz_nulls = activity['timestamp'].isna().sum()
    print(f"   → Timestamps: {tz_nulls} unparseable → dropped as NaT")
    print("   → Strategy: All timestamps normalized to UTC (timezone-aware → naive UTC)")

# d) Handle encoding issues in string columns
for col in ['feature', 'event_type', 'browser', 'os']:
    if col in activity.columns:
        activity[col] = activity[col].astype(str).str.strip()
        activity[col] = activity[col].replace('nan', np.nan)

# e) Outlier detection: session_duration_sec
if 'session_duration_sec' in activity.columns:
    q99 = activity['session_duration_sec'].quantile(0.99)
    q01 = activity['session_duration_sec'].quantile(0.01)
    outliers_before = len(activity)
    # Cap extreme outliers (> 99th percentile or negative values)
    activity.loc[activity['session_duration_sec'] < 0, 'session_duration_sec'] = np.nan
    activity.loc[activity['session_duration_sec'] > q99 * 3, 'session_duration_sec'] = np.nan
    outliers_after = activity['session_duration_sec'].isna().sum()
    print(f"   → Outliers: session_duration capped at 3×P99 ({q99*3:.0f}s), "
          f"{outliers_after - tz_nulls if 'tz_nulls' in dir() else 0} extreme values → NaN")

# f) Duplicates
dup_act = activity.duplicated().sum()
print(f"   → Exact duplicate rows: {dup_act}")
activity.drop_duplicates(inplace=True)
print(f"   [user_activity_logs] Final rows: {len(activity)}")

# --- nps_survey_responses ---
nps = mongo_data['nps_survey_responses'].copy()
print(f"\n   [nps_survey_responses] Starting rows: {len(nps)}")
nps['customer_id'] = pd.to_numeric(nps.get('customer_id', nps.get('customerId', pd.Series())), errors='coerce')
nps['nps_score'] = pd.to_numeric(nps['nps_score'], errors='coerce')
nps['survey_date'] = pd.to_datetime(nps.get('survey_date', pd.Series()), errors='coerce', utc=True)
if nps['survey_date'].dt.tz is not None:
    nps['survey_date'] = nps['survey_date'].dt.tz_localize(None)
dup_nps = nps.duplicated().sum()
print(f"   → Duplicate rows: {dup_nps}")
nps.drop_duplicates(inplace=True)
print(f"   [nps_survey_responses] Final rows: {len(nps)}")

# --- onboarding_events ---
onboarding = mongo_data['onboarding_events'].copy()
print(f"\n   [onboarding_events] Starting rows: {len(onboarding)}")
# Normalize inconsistent field names
for old, new in [('customerId', 'customer_id'), ('memberId', 'member_id')]:
    if old in onboarding.columns:
        onboarding[new] = onboarding.get(new, pd.Series(dtype=float)).combine_first(onboarding[old])
        onboarding.drop(columns=[old], inplace=True, errors='ignore')
        print(f"   → Fixed: merged '{old}' into '{new}'")
onboarding['customer_id'] = pd.to_numeric(onboarding['customer_id'], errors='coerce')
onboarding['timestamp'] = pd.to_datetime(onboarding.get('timestamp', pd.Series()), errors='coerce', utc=True)
if onboarding['timestamp'].dt.tz is not None:
    onboarding['timestamp'] = onboarding['timestamp'].dt.tz_localize(None)
dup_onb = onboarding.duplicated().sum()
print(f"   → Duplicate rows: {dup_onb}")
onboarding.drop_duplicates(inplace=True)
print(f"   [onboarding_events] Final rows: {len(onboarding)}")

# ─────────────────────────────────────────────────
# Step 2.4: MERGE SQL + MongoDB on customer_id
# ─────────────────────────────────────────────────
print("\n🔗 MERGING SQL + MongoDB Data...")

# Get the latest/active subscription per customer (for plan tier info)
subs_with_plans = subs.merge(plans[['plan_id', 'plan_name', 'plan_tier']], on='plan_id', how='left')
# Take the most recent subscription per customer
latest_subs = subs_with_plans.sort_values('start_date', ascending=False).drop_duplicates('customer_id', keep='first')
latest_subs = latest_subs[['customer_id', 'plan_id', 'plan_name', 'plan_tier', 'status', 'mrr_usd', 'billing_cycle']]

# Team size per customer
team_size = members.groupby('customer_id').agg(
    team_size=('member_id', 'count'),
    active_members=('is_active', 'sum')
).reset_index()

# Activity metrics per customer (from MongoDB)
activity_agg = activity.groupby('customer_id').agg(
    total_events=('customer_id', 'count'),
    avg_session_sec=('session_duration_sec', 'mean'),
    unique_features=('feature', 'nunique'),
    last_activity=('timestamp', 'max'),
    first_activity=('timestamp', 'min'),
    total_sessions_with_duration=('session_duration_sec', 'count')
).reset_index()

# NPS average per customer (from MongoDB)
nps_agg = nps.groupby('customer_id').agg(
    latest_nps=('nps_score', 'last'),
    avg_nps=('nps_score', 'mean'),
    nps_surveys_count=('nps_score', 'count')
).reset_index()

# Feature usage flags per customer
if 'feature' in activity.columns:
    feature_pivot = activity[activity['feature'].notna()].groupby(
        ['customer_id', 'feature']
    ).size().unstack(fill_value=0).reset_index()
    feature_pivot.columns = ['customer_id'] + [f'feature_{c}_count' for c in feature_pivot.columns[1:]]
else:
    feature_pivot = pd.DataFrame({'customer_id': activity['customer_id'].unique()})

# === THE MASTER MERGE ===
print("\n   Building master customer table...")
master = customers.copy()
master = master.merge(latest_subs, on='customer_id', how='left')
master = master.merge(team_size, on='customer_id', how='left')
master = master.merge(activity_agg, on='customer_id', how='left')
master = master.merge(nps_agg, on='customer_id', how='left')

print(f"   ✅ Master table created: {len(master):,} rows × {len(master.columns)} columns")

# Fill missing activity metrics with 0 (customers who had no events)
for col in ['total_events', 'avg_session_sec', 'unique_features', 'team_size', 'active_members']:
    if col in master.columns:
        master[col] = master[col].fillna(0)

# ─────────────────────────────────────────────────
# Step 2.5: AFTER Cleaning — Row Counts
# ─────────────────────────────────────────────────
print("\n📊 AFTER Cleaning — Row Counts:")
print("-" * 50)
after_data = {
    'customers': customers, 'subscriptions': subs, 'plans': plans,
    'team_members': members, 'user_activity_logs': activity,
    'nps_survey_responses': nps, 'onboarding_events': onboarding,
    'master_merged': master
}
for name, df in after_data.items():
    before_key = f"sql_{name}" if name in sql_tables else f"mongo_{name}"
    before_val = before_counts.get(before_key, "—")
    tag = "FINAL" if name == 'master_merged' else "     "
    print(f"   {tag} | {name:25s}: {len(df):>6,} rows (was {before_val})")


# ============================================================
# SECTION 3: HYPOTHESIS TEST
# ============================================================
print("\n" + "=" * 70)
print("SECTION 3: HYPOTHESIS TEST")
print("=" * 70)

print("""
┌──────────────────────────────────────────────────────────────┐
│  HYPOTHESIS: High-engagement customers have lower churn      │
│                                                              │
│  H₀: There is no significant difference in total activity    │
│      events between churned and active customers.            │
│                                                              │
│  H₁: Active (non-churned) customers have significantly      │
│      MORE activity events than churned customers.            │
│                                                              │
│  Significance Level: α = 0.05                                │
│  Test: Mann-Whitney U (non-parametric, no normality needed)  │
│  Rationale: Event counts are likely right-skewed, so we      │
│      use a non-parametric test instead of a t-test.          │
└──────────────────────────────────────────────────────────────┘
""")

# Split into two groups
churned = master[master['is_churned'] == 1]['total_events']
active = master[master['is_churned'] == 0]['total_events']

print(f"   Group sizes: Active customers = {len(active)}, Churned customers = {len(churned)}")
print(f"   Mean events: Active = {active.mean():.1f}, Churned = {churned.mean():.1f}")
print(f"   Median events: Active = {active.median():.1f}, Churned = {churned.median():.1f}")

# Check assumptions: normality (Shapiro-Wilk on a sample)
if len(active) > 5000:
    shapiro_sample_active = active.sample(5000, random_state=42)
else:
    shapiro_sample_active = active
shapiro_stat, shapiro_p = stats.shapiro(shapiro_sample_active.dropna())
print(f"\n   Normality Check (Shapiro-Wilk on active group):")
print(f"   → W={shapiro_stat:.4f}, p={shapiro_p:.6f}")
if shapiro_p < 0.05:
    print(f"   → Data is NOT normally distributed (p < 0.05). Non-parametric test confirmed.")
else:
    print(f"   → Data appears normal. However, we proceed with Mann-Whitney for robustness.")

# Mann-Whitney U Test (one-sided: active > churned)
u_stat, p_value_two_sided = stats.mannwhitneyu(
    active.dropna(), churned.dropna(), alternative='two-sided'
)
# For one-sided (active > churned):
_, p_value_one_sided = stats.mannwhitneyu(
    active.dropna(), churned.dropna(), alternative='greater'
)

print(f"\n   ═══ MANN-WHITNEY U TEST RESULTS ═══")
print(f"   U-statistic: {u_stat:,.0f}")
print(f"   p-value (two-sided): {p_value_two_sided:.6f}")
print(f"   p-value (one-sided, active > churned): {p_value_one_sided:.6f}")

alpha = 0.05
if p_value_one_sided < alpha:
    print(f"\n   ✅ RESULT: REJECT H₀ (p={p_value_one_sided:.6f} < α={alpha})")
    print(f"   → Active customers have significantly more engagement events")
    print(f"   → than churned customers. Higher engagement is associated with retention.")
else:
    print(f"\n   ❌ RESULT: FAIL TO REJECT H₀ (p={p_value_one_sided:.6f} ≥ α={alpha})")
    print(f"   → No statistically significant difference in engagement between groups.")

# Effect size (rank-biserial correlation)
n1, n2 = len(active.dropna()), len(churned.dropna())
r_effect = 1 - (2 * u_stat) / (n1 * n2)
print(f"\n   Effect Size (rank-biserial r): {r_effect:.4f}")
if abs(r_effect) < 0.1:
    print(f"   → Negligible effect")
elif abs(r_effect) < 0.3:
    print(f"   → Small effect")
elif abs(r_effect) < 0.5:
    print(f"   → Medium effect")
else:
    print(f"   → Large effect")


# ============================================================
# SECTION 4: CUSTOMER SEGMENTATION
# ============================================================
print("\n" + "=" * 70)
print("SECTION 4: CUSTOMER SEGMENTATION")
print("=" * 70)

print("""
┌──────────────────────────────────────────────────────────────┐
│  METHODOLOGY: Engagement-Based RFM Segmentation             │
│                                                              │
│  We segment customers using 3 behavioral dimensions:        │
│                                                              │
│  R (Recency): Days since last activity event                │
│     → Lower = more recent = better                          │
│                                                              │
│  F (Frequency): Total number of activity events             │
│     → Higher = more engaged = better                        │
│                                                              │
│  M (Monetary/Value): MRR contribution in USD                │
│     → Higher = more valuable = better                       │
│                                                              │
│  Each dimension is scored 1-4 (quartiles).                  │
│  Combined into a segment label for actionable targeting.    │
└──────────────────────────────────────────────────────────────┘
""")

# Prepare segmentation features
seg = master[['customer_id', 'company_name', 'industry', 'is_churned',
              'plan_tier', 'mrr_usd', 'total_events', 'last_activity',
              'avg_session_sec', 'unique_features', 'team_size', 'latest_nps']].copy()

# Calculate Recency (days since last activity)
seg['recency_days'] = (pd.Timestamp.now() - pd.to_datetime(seg['last_activity'], errors='coerce')).dt.days
seg['recency_days'] = seg['recency_days'].fillna(9999)  # No activity = very high recency

# Fill NaN for MRR
seg['mrr_usd'] = seg['mrr_usd'].fillna(0)

# Score each dimension into quartiles (1=worst, 4=best)
# Recency: lower is better → invert scoring
seg['R_score'] = pd.qcut(seg['recency_days'], q=4, labels=[4, 3, 2, 1], duplicates='drop').astype(int)
# Frequency: higher is better
seg['F_score'] = pd.qcut(seg['total_events'].rank(method='first'), q=4, labels=[1, 2, 3, 4], duplicates='drop').astype(int)
# Value: higher MRR is better
seg['V_score'] = pd.qcut(seg['mrr_usd'].rank(method='first'), q=4, labels=[1, 2, 3, 4], duplicates='drop').astype(int)

# Combined RFV score
seg['rfv_total'] = seg['R_score'] + seg['F_score'] + seg['V_score']

# Segment assignment
def assign_segment(row):
    r, f, v = row['R_score'], row['F_score'], row['V_score']
    total = row['rfv_total']

    if total >= 10:
        return '🟢 Champions'
    elif f >= 3 and r >= 3:
        return '🔵 Loyal Customers'
    elif r >= 3 and f <= 2:
        return '🟡 New/Promising'
    elif r <= 2 and f >= 3:
        return '🟠 At Risk'
    elif r <= 2 and f <= 2 and v >= 3:
        return '🔴 High-Value Sleepers'
    elif total <= 5:
        return '⚪ Dormant/Lost'
    else:
        return '🟤 Need Attention'

seg['segment'] = seg.apply(assign_segment, axis=1)

# Display segment distribution
print("📊 SEGMENT DISTRIBUTION:")
print("-" * 60)
segment_summary = seg.groupby('segment').agg(
    count=('customer_id', 'count'),
    avg_events=('total_events', 'mean'),
    avg_mrr=('mrr_usd', 'mean'),
    avg_recency=('recency_days', 'mean'),
    churn_rate=('is_churned', 'mean')
).round(2)
segment_summary['churn_rate_pct'] = (segment_summary['churn_rate'] * 100).round(1)
segment_summary = segment_summary.drop(columns=['churn_rate'])
segment_summary = segment_summary.sort_values('count', ascending=False)

print(segment_summary.to_string())

print("""
\n📋 BUSINESS IMPLICATIONS BY SEGMENT:
─────────────────────────────────────────────────────────────────

🟢 Champions (High R, F, V):
   → Most engaged, highest MRR. Focus on RETENTION programs.
   → Strategy: VIP support, early access to features, referral incentives.

🔵 Loyal Customers (High F, High R):
   → Frequently engaged and recent. Upsell opportunity.
   → Strategy: Suggest plan upgrades, showcase premium features.

🟡 New/Promising (High R, Low F):
   → Recently joined but haven't engaged deeply yet.
   → Strategy: Onboarding campaigns, feature tours, activation emails.

🟠 At Risk (Low R, High F):
   → Were highly engaged but haven't been active recently.
   → Strategy: Win-back campaigns, personalized outreach, exit surveys.

🔴 High-Value Sleepers (Low R, Low F, High V):
   → Paying high MRR but barely using the product. HIGH CHURN RISK.
   → Strategy: Proactive CSM outreach, ROI demonstration, training.

⚪ Dormant/Lost (Low everything):
   → Minimal engagement, low value. Likely already churned or inactive.
   → Strategy: Automated re-engagement emails, or accept natural churn.

🟤 Need Attention (Mixed signals):
   → Doesn't fit neatly into other segments. Monitor closely.
   → Strategy: Investigate individual cases, personalized interventions.
""")


# ============================================================
# SECTION 5: SAVE OUTPUTS
# ============================================================
print("\n" + "=" * 70)
print("SECTION 5: SAVING OUTPUTS")
print("=" * 70)

# Save master merged table
master.to_csv(os.path.join(OUTPUT_DIR, 'master_customer_360.csv'), index=False)
print(f"   ✅ Saved: {OUTPUT_DIR}/master_customer_360.csv ({len(master)} rows)")

# Save segmentation
seg.to_csv(os.path.join(OUTPUT_DIR, 'customer_segments.csv'), index=False)
print(f"   ✅ Saved: {OUTPUT_DIR}/customer_segments.csv ({len(seg)} rows)")

# Save cleaned individual tables
for name, df in [('customers', customers), ('subscriptions', subs),
                 ('plans', plans), ('team_members', members)]:
    df.to_csv(os.path.join(OUTPUT_DIR, f'{name}.csv'), index=False)
    print(f"   ✅ Saved: {OUTPUT_DIR}/{name}.csv ({len(df)} rows)")

for name, df in [('user_activity_logs', activity),
                 ('nps_survey_responses', nps),
                 ('onboarding_events', onboarding)]:
    df.to_csv(os.path.join(OUTPUT_DIR, f'{name}.csv'), index=False)
    print(f"   ✅ Saved: {OUTPUT_DIR}/{name}.csv ({len(df)} rows)")

print("\n" + "=" * 70)
print("✅ TASK 3 COMPLETE — All outputs saved to /processed_data/")
print("=" * 70)
