# 📊 Meta Ads Creative Performance Pipeline
### Facebook Ads → BigQuery → Looker Studio

> A production-grade ETL pipeline that pulls Facebook Ads data daily, enriches it with creative metadata, loads it into BigQuery, and powers a Looker Studio creative performance dashboard — tracking winner rates, angles, products, and creative velocity.

---

## 📁 Project Structure

```
meta-ads-pipeline/
├── fb_etl.py                  # Main ETL script (run daily)
├── fb_extraction.log          # Auto-generated runtime log
├── data/
│   └── raw/                   # Temporary Parquet staging files
│       └── fb_{acc}_{date}.parquet
└── bq_view.sql                # BigQuery view definition (creative_performance_view)
```

---

## 🏗️ Architecture Overview

```
Facebook Marketing API
        │
        ▼
  fb_etl.py (Python)
  ┌─────────────────────────────────────────┐
  │  1. Fetch ad-level insights (daily)     │
  │  2. Parse purchases, ATC, checkout      │
  │  3. Async enrich creative URLs + thumbs │
  │  4. Write to Parquet (zstd compressed)  │
  └──────────────────┬──────────────────────┘
                     │ WRITE_APPEND
                     ▼
        BigQuery: growthruben.Meta.Facebook_Ads
                     │
                     ▼
        BigQuery View: creative_performance_view
        (dedup + cast + naming parse + winner logic)
                     │
                     ▼
        Looker Studio Dashboard
        (Creative KPIs, Winner Rate, Angles, Products)
```

---

## ⚙️ ETL Script — `fb_etl.py`

### What It Does

| Step | Description |
|------|-------------|
| **1. Auth** | Initialises Facebook Business SDK with App ID, App Secret, Access Token |
| **2. Fetch** | Pulls ad-level insights for `yesterday` across all configured Ad Accounts |
| **3. Parse** | Extracts purchases, revenue, ATC, checkout, video metrics per row |
| **4. Missing date fill** | Re-fetches any dates with gaps to ensure complete coverage |
| **5. Creative enrichment** | Async fetch of `Creative_Facebook_URL` and `Creative_thumbnail_URL` via Graph API (30 concurrent workers) |
| **6. Parquet write** | Saves staged data as `.parquet` with `zstd` compression |
| **7. BigQuery upload** | `WRITE_APPEND` load job to `growthruben.Meta.Facebook_Ads` |

### Key Config Variables

```python
# ── Facebook ────────────────────────────────
FACEBOOK_APP_ID       = "..."          # Your Meta App ID
FACEBOOK_APP_SECRET   = "..."          # Your Meta App Secret
FACEBOOK_ACCESS_TOKEN = "..."          # Long-lived system user token

AD_ACCOUNTS = [
    "1294843742357995",                # Account 1
    "1631785441072727",                # Account 2
]

# ── BigQuery ─────────────────────────────────
BQ_PROJECT = "growthruben"
BQ_DATASET = "Meta"
BQ_TABLE   = "Facebook_Ads"

# ── Performance ──────────────────────────────
MAX_WORKERS      = 2    # Parallel ad accounts
CREATIVE_WORKERS = 30   # Concurrent async creative fetches
```

### Metrics Extracted per Ad per Day

| Field | Source |
|-------|--------|
| `amount_spent` | Facebook `spend` field |
| `Clicks_all` | Facebook `clicks` field |
| `Link_Clicks` | Facebook `inline_link_clicks` |
| `Impressions` | Facebook `impressions` |
| `Purchases` | `actions[action_type=purchase]` |
| `Purchases_conversion_value` | `action_values[purchase]` |
| `Initiated_Checkout` | `omni_initiated_checkout` or `fb_pixel_initiate_checkout` |
| `Add_to_Cart` | `add_to_cart` action |
| `Video_plays` | `video_view` action |
| `Three_second_video_plays` | `video_view` (3s) |
| `Video_plays_at_100_percent` | `video_p100_watched_actions` |
| `Video_average_play_time` | `video_avg_time_watched_actions` (ms → seconds) |
| `Creative_Facebook_URL` | Graph API: `effective_object_story_id` |
| `Creative_thumbnail_URL` | Graph API: `thumbnail_url` or `image_url` |

### Retry Logic

The script automatically retries on these Facebook API error codes:

| Code | Meaning |
|------|---------|
| 1, 2 | Unknown / service error |
| 4, 17 | Rate limit / call volume |
| 32 | Page-level throttle |
| 190 | Access token error |
| 613 | Custom spend limit |

Retries use exponential backoff: `min(2^attempt, 60)` seconds, up to 5 attempts.

---

## 🗃️ BigQuery Table — `growthruben.Meta.Facebook_Ads`

### Schema

| Column | Type | Notes |
|--------|------|-------|
| `Account_ID` | STRING | Ad Account ID (no `act_` prefix) |
| `Campaign_ID` | STRING | |
| `Campaign_name` | STRING | |
| `Ad_ID` | STRING | |
| `Ad_Name` | STRING | Full naming convention string |
| `Adset_ID` | STRING | |
| `Adset_Name` | STRING | |
| `Clicks_all` | STRING | Cast to FLOAT64 in view |
| `Link_Clicks` | STRING | Cast to FLOAT64 in view |
| `amount_spent` | STRING | Cast to FLOAT64 in view |
| `Impressions` | STRING | Cast to FLOAT64 in view |
| `Video_plays` | STRING | |
| `Video_plays_at_100_percent` | STRING | |
| `Three_second_video_plays` | STRING | |
| `Video_average_play_time` | STRING | Seconds (converted from ms) |
| `Purchases` | STRING | |
| `Purchases_conversion_value` | STRING | |
| `Initiated_Checkout` | STRING | |
| `Initiated_Checkout_Value` | STRING | |
| `Add_to_Cart` | STRING | |
| `Add_to_Cart_Value` | STRING | |
| `Creative_Facebook_URL` | STRING | Permalink to FB post |
| `Creative_thumbnail_URL` | STRING | Image preview URL |
| `Country` | STRING | Reserved (not populated by ETL) |
| `Currency` | STRING | Reserved (not populated by ETL) |
| `Adset_creation_time` | DATE | Reserved (null by default) |
| `day` | DATE | Reporting date |

> **Note:** All numeric fields are stored as STRING and cast inside the BigQuery view. This is intentional for safe schema evolution.

---

## 📐 Ad Naming Convention

The pipeline parses `Ad_Name` by splitting on ` _ ` (space-underscore-space). Each position maps to a creative dimension:

```
Position  Field               Example
──────────────────────────────────────────────────
[0]       awareness           TOP / MOF / BOF
[1]       campaign_id         C01
[2]       creative_id         I02
[3]       creative_variation  I02 or I02.1 (variation)
[4]       creative_strategist Sarah
[5]       creative_type       UGC / Static / Video
[6]       brief_type          Testimonial / Demo
[7]       angle               PainPoint / Social Proof
[8]       product             HerbA / DrumKit
[9]       ad_launch_date      2024-11-15  (parsed as DATE)
[10]      market              US / UK / AU
[11]      editor              John
[12]      cta                 ShopNow / LearnMore
```

**Variation detection:** If `creative_variation` contains a dot (e.g. `I02.1`), it is labelled as `Variation`.  
**Revived detection:** If the same `creative_id` is re-launched after its first-ever `ad_launch_date`, it is labelled `Revived Creative`.

---

## 🔍 BigQuery View — `creative_performance_view`

### What the View Builds

The view is structured in 5 CTEs:

```sql
url_dedup      → MAX() creative URLs per Ad_Name (removes duplicate rows)
daily          → Casts all metrics to FLOAT64, parses naming convention
lifetime       → SUM aggregates per Ad_Name (spend, revenue, purchases, ROAS, CPA)
first_launch   → MIN(ad_launch_date) per creative_id (for Revived logic)
[final SELECT] → Joins all above + winner flag + creative label
```

### Winner Logic

```sql
is_winner = TRUE  when:
  lifetime_spend  >= 1000   AND
  lifetime_roas   >= 2.0
```

### Creative Label Logic

```sql
'Variation'        → creative_variation LIKE 'X.1', 'X.2', etc.
'Revived Creative' → same creative_id re-launched after its first launch date
'New Creative'     → everything else
```

### Key Computed Columns

| Column | Formula |
|--------|---------|
| `lifetime_spend` | `SUM(amount_spent)` per Ad_Name |
| `lifetime_revenue` | `SUM(revenue)` per Ad_Name |
| `lifetime_roas` | `lifetime_revenue / lifetime_spend` |
| `lifetime_cpa` | `lifetime_spend / lifetime_purchases` |
| `launch_week` | `DATE_TRUNC(ad_launch_date, WEEK(MONDAY))` |
| `week_start` | `DATE_TRUNC(day, WEEK(MONDAY))` |
| `creative_facebook_url` | Fixed permalink from `{page_id}/posts/{post_id}` |

---

## 📊 Looker Studio Dashboard Setup

### Step 1 — Connect BigQuery as Data Source

1. Open [Looker Studio](https://lookerstudio.google.com/u/0/reporting/cb6ff94b-5304-494e-84bf-377f731aece3/page/ZZdsF/edit)
2. Click **Create → Data Source**
3. Select **BigQuery** connector
4. Choose:
   - **Project:** `growthruben`
   - **Dataset:** `Meta`
   - **Table/View:** `creative_performance_view`
5. Click **Connect → Add to Report**

### Step 2 — Calculated Fields to Create in Looker Studio

Create these as **Calculated Fields** inside Looker Studio:

| Field Name | Formula | Format |
|------------|---------|--------|
| `Win Rate` | `COUNTIF(is_winner, TRUE) / COUNT(Ad_Name)` | Percent |
| `CTR` | `link_clicks / impressions` | Percent |
| `Thumb Stop Rate` | `video_plays_3s / impressions` | Percent |
| `Hook Rate` | `video_plays_3s / impressions` | Percent |
| `Completion Rate` | `video_plays_100pct / video_plays` | Percent |
| `ROAS` | `lifetime_revenue / lifetime_spend` | Decimal (2dp) |
| `CPA` | `lifetime_spend / lifetime_purchases` | Currency |
| `% New Creatives` | `COUNTIF(creative_label, "New Creative") / COUNT(Ad_Name)` | Percent |
| `% Variations` | `COUNTIF(creative_label, "Variation") / COUNT(Ad_Name)` | Percent |
| `% Revived` | `COUNTIF(creative_label, "Revived Creative") / COUNT(Ad_Name)` | Percent |

### Step 3 — Dashboard Pages & Charts

#### Page 1 — Creative Overview

| Chart | Dimension | Metric |
|-------|-----------|--------|
| Scorecard | — | Total creatives launched, Win Rate, Avg ROAS |
| Time series | `launch_week` | Creatives launched per week |
| Bar chart | `creative_label` | Count of creatives (New / Variation / Revived) |
| Pie chart | `creative_label` | % split New / Variation / Revived |

#### Page 2 — Winner Analysis

| Chart | Dimension | Metric |
|-------|-----------|--------|
| Scorecard | — | Total winners, Win Rate %, Avg winner ROAS |
| Table | `Ad_Name`, `creative_type`, `angle`, `product` | `lifetime_spend`, `lifetime_roas`, `is_winner` |
| Bar chart | `angle` | Win Rate per angle |
| Bar chart | `product` | Win Rate per product |
| Bar chart | `creative_strategist` | Win Rate per strategist |

#### Page 3 — Angle & Product Performance

| Chart | Dimension | Metric |
|-------|-----------|--------|
| Bar chart | `angle` | `SUM(lifetime_spend)`, count of creatives |
| Bar chart | `product` | `SUM(lifetime_spend)`, count of creatives |
| Table | `angle` | % of total creatives, % of total spend, Win Rate |
| Table | `product` | % of total creatives, % of total spend, Win Rate |

#### Page 4 — Weekly Creative Velocity

| Chart | Dimension | Metric |
|-------|-----------|--------|
| Line chart | `launch_week` | Creatives launched, New only, Variations only |
| Bar chart | `launch_week` | `SUM(amount_spent)` |
| Table | `launch_week`, `creative_type` | Count breakdown |

#### Page 5 — Creative Library

| Chart | Dimension | Metric |
|-------|-----------|--------|
| Table with image | `creative_thumbnail_url`, `Ad_Name` | `lifetime_spend`, `lifetime_roas`, `is_winner`, `creative_label` |
| Filter control | `angle`, `product`, `creative_type`, `is_winner` | — |

> **Tip:** Use **Image** type on `creative_thumbnail_url` field in table settings to render ad thumbnails inline.

### Step 4 — Filters & Controls to Add

Add these **Filter Controls** to every page header:

- `day` → Date Range picker
- `angle` → Dropdown
- `product` → Dropdown
- `creative_type` → Dropdown
- `market` → Dropdown
- `is_winner` → Dropdown (TRUE / FALSE)
- `creative_label` → Dropdown (New / Variation / Revived)

---

## 🔑 Metrics Reference for Dashboard

| Metric | How to Calculate in Looker Studio |
|--------|-----------------------------------|
| **Winning Rate** | Winners ÷ Total unique ads |
| **Winning Rate per Angle** | Filter by angle, apply Win Rate formula |
| **Winning Rate per Product** | Filter by product, apply Win Rate formula |
| **Creatives Launched per Week** | COUNT(Ad_Name) grouped by `launch_week` |
| **New Creatives Tested per Week** | COUNT where `creative_label = 'New Creative'` by `launch_week` |
| **% New Creatives Launched** | New Creative count ÷ Total count |
| **% Variations** | Variation count ÷ Total count |
| **% by Type** | Breakdown across New / Variation / Revived |
| **Best Angle (spend %)** | Angle with highest `SUM(lifetime_spend)` share |
| **Best Angle (creative %)** | Angle with highest creative count share |
| **Best Product (spend %)** | Product with highest `SUM(lifetime_spend)` share |
| **Best Product (creative %)** | Product with highest creative count share |

---

## 🚀 Running the Pipeline

### First-time Setup

```bash
# 1. Install dependencies
pip install facebook-business pyarrow aiohttp google-cloud-bigquery

# 2. Run the script (pulls yesterday's data by default)
python fb_etl.py
```

### Scheduled Daily Run (n8n or Cron)

**Cron (Linux/Mac):**
```bash
# Run every day at 6:00 AM PKT (1:00 AM UTC)
0 1 * * * /usr/bin/python3 /path/to/fb_etl.py >> /var/log/fb_etl.log 2>&1
```

**n8n:**
1. Create a **Schedule Trigger** node → daily at 01:00 UTC
2. Connect to **Execute Command** node → `python3 /path/to/fb_etl.py`
3. Add **Slack/Email notification** on success/failure

### Backfilling Historical Data

To load a custom date range, edit the `main()` function:

```python
def main():
    START = "2024-11-01"   # ← change this
    END   = "2024-11-30"   # ← change this
    ...
```

Then run: `python fb_etl.py`

---

## 🛠️ Maintaining the BigQuery View

Create or replace the view using `bq_view.sql`:

```bash
bq query \
  --project_id=growthruben \
  --use_legacy_sql=false \
  "$(cat bq_view.sql | sed 's/^/CREATE OR REPLACE VIEW growthruben.Meta.creative_performance_view AS\n/' | head -1)$(cat bq_view.sql)"
```

Or paste directly into the BigQuery console:

```sql
CREATE OR REPLACE VIEW `growthruben.Meta.creative_performance_view` AS
-- [paste full contents of bq_view.sql here]
```

---

## 🔒 Security Notes

| Secret | Location | Action Required |
|--------|----------|-----------------|
| `FACEBOOK_ACCESS_TOKEN` | Hardcoded in script | **Move to env var or Secret Manager** |
| `FACEBOOK_APP_SECRET` | Hardcoded in script | **Move to env var or Secret Manager** |
| `SERVICE_ACCOUNT_INFO` | Hardcoded in script | **Move to .json keyfile or Secret Manager** |

**Recommended:**
```python
import os
FACEBOOK_ACCESS_TOKEN = os.environ["FB_ACCESS_TOKEN"]
```

---

## 🐛 Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `FacebookRequestError code 190` | Expired access token | Regenerate long-lived token in Meta Business Manager |
| `No data` for an account | Account has no active ads that day | Normal — script will log and skip |
| Creative URLs empty | Graph API creative fields unavailable | Check token has `ads_read` permission |
| BigQuery upload fails | Service account lacks permissions | Grant `roles/bigquery.dataEditor` to service account |
| Duplicate rows in BQ | Script ran twice for same date | Run dedup query: `DELETE FROM ... WHERE day = 'YYYY-MM-DD'` before re-running |

---

## 📦 Dependencies

```
facebook-business    >= 17.0.0
pyarrow              >= 14.0.0
aiohttp              >= 3.9.0
google-cloud-bigquery >= 3.11.0
google-auth          >= 2.23.0
```

---

## 📋 Changelog

| Date | Change |
|------|--------|
| 2024-03-24 | Fixed `amount_spent` cast — was rounding to wrong type |
| 2024-03-24 | Fixed `Link_Clicks` — now uses `inline_link_clicks` (not `clicks`) |
| 2024-03-24 | Fixed `Video_average_play_time` — ms → seconds conversion |
| 2024-03-24 | Fixed `Clicks_all` — proper float cast |

---

*Built for the growthruben Meta Ads pipeline. Maintained by Buildberg Analytics.*
