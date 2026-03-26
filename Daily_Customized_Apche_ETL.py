!pip install facebook-business pyarrow aiohttp google-cloud-bigquery
import os
import time
import asyncio
import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, timedelta
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

from google.cloud import bigquery
from google.oauth2 import service_account

# =============================================
# CONFIG
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler("fb_extraction.log")]
)
logger = logging.getLogger(__name__)

FACEBOOK_APP_ID       = "1271---1142868"
FACEBOOK_APP_SECRET   = "486da82d0e---c00bd9ebfc56"
FACEBOOK_ACCESS_TOKEN = "EAASEauk6H1QB----Sx3CCBSyseguATmo6DsSioq4kfVxtXA6VJaypRIl26TPT---7EVrZBLFZCyv537DLef1wFNOyN0EN20vau0EZCudG86LkZAZAZAkaPpCDYxRXWWBN0KD4KYnRZA2M8swMzuO"
AD_ACCOUNTS = [
    "12948---995",
    "16317---072727",
]

RAW_DIR = "./data/raw/"
os.makedirs(RAW_DIR, exist_ok=True)

BQ_PROJECT = "growthruben"
BQ_DATASET = "Meta"
BQ_TABLE   = "Facebook_Ads"

# =============================================
# INLINE SERVICE ACCOUNT CREDENTIALS
# =============================================
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "gro--uben",
    "private_key_id": "8c36b----60b4c7e8eb2a",
    "private_key": (
        "-----BEGIN PRIVATE KEY-----\n"
        "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC47qwUoHO+iVTL\n"
        "Br9hWCdnot2J7QWTvtTctxsUGa6TwHv2PRfgesMeL651M6Gv0jPGqlDGNgZEsFBs\n"
      ----
        "Bk6gTP8P3KGc1smGU41ryn8jx7863exNEG5jRoYRyz4azpaUNrxVgSge2Z0w25WT\n"
        "tv+LDjYu1QKBgBwC8eG1CAp63nFfENWrfO8fz+IAUa+Xy1/pZpdx+raWUSi4yynP\n"
        "hjns49OWCgYEA2b6DR3uTxWjgJdicn1pe\n"
        "zU1urmX7NGN67MA/sVFQRKxPx7DPaHRLAmp1DV63KQeiHFLuVHVAUmqCJFsMnK7e\n"
        "um0R96897l35IzB/GjDZkSXKYQHbNSJgzZXvpF2o/iVGFhtIxX8Bfbp1sUJdWNF/\n"
        "kQSLMOy3ZEHEdCdo/l64hE4=\n"
        "-----END PRIVATE KEY-----\n"
    ),
    "client_email": "growthr----ceaccount.com",
    "client_id": "11---470",
    "auth_uri": "https://accou---h2/auth",
    "token_uri": "https://--h2.g----is.com/token",
    "auth_provider_x509_cert_url": "https://ww--uth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v----n.iam.gserviceaccount.com",
    "universe_domain": "--com",
}

MAX_WORKERS      = 2
CREATIVE_WORKERS = 30

# =============================================
# HELPERS
# =============================================
def safe_float(x, default=0.0):
    try: return float(x) if x is not None else default
    except: return default

def safe_date(s):
    if not s: return None
    try: return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except: return None

def get_dates(start: str, end: str):
    s = datetime.strptime(start, "%Y-%m-%d").date()
    e = datetime.strptime(end, "%Y-%m-%d").date()
    return [s + timedelta(days=i) for i in range((e-s).days + 1)]

def get_action_value(actions_list, action_type):
    if not actions_list: return 0
    for a in actions_list:
        if a.get("action_type") == action_type:
            try: return int(a.get("value", 0))
            except: return 0
    return 0

def get_action_revenue(action_values_list, cost_per_action_list, count, action_types_to_try):
    revenue = 0.0
    for av in action_values_list:
        if av.get("action_type") in action_types_to_try:
            try:
                val = float(av.get("value", 0))
                if val > 0:
                    return val
            except: pass
    if count > 0:
        for c in cost_per_action_list:
            if c.get("action_type") in action_types_to_try:
                try:
                    cpa = float(c.get("value", 0))
                    return round(count * cpa, 2)
                except: pass
    return revenue

# =============================================
# FIELDS
# =============================================
FIELDS = [
    "campaign_id","campaign_name","adset_id","adset_name",
    "ad_id","ad_name","spend","clicks","impressions",
    "actions","action_values","cost_per_action_type",
    "video_play_actions","video_p100_watched_actions",
    "video_avg_time_watched_actions","date_start",
    "inline_link_clicks",
]

# =============================================
# ROW PROCESSING
# =============================================
def process_row(raw: Dict, acc_id: str) -> Dict:
    actions         = raw.get("actions", []) or []
    action_values   = raw.get("action_values", []) or []
    cost_per_action = raw.get("cost_per_action_type", []) or []
    video_play      = raw.get("video_play_actions", []) or []
    video_100       = raw.get("video_p100_watched_actions", []) or []
    video_avg       = raw.get("video_avg_time_watched_actions", []) or []

    purchases = get_action_value(actions, "purchase")
    purchase_revenue = get_action_revenue(action_values, cost_per_action, purchases, ["purchase", "omni_purchase"])

    initiated_checkout = (
        get_action_value(actions, "omni_initiated_checkout") or
        get_action_value(actions, "offsite_conversion.fb_pixel_initiate_checkout") or
        get_action_value(actions, "initiations")
    )
    initiated_checkout_value = get_action_revenue(
        action_values, cost_per_action, initiated_checkout,
        ["omni_initiated_checkout", "offsite_conversion.fb_pixel_initiate_checkout", "initiations"]
    )

    add_to_cart = get_action_value(actions, "add_to_cart")
    add_to_cart_value = get_action_revenue(
        action_values, cost_per_action, add_to_cart,
        ["omni_add_to_cart", "add_to_cart"]
    )

    link_clicks       = round(safe_float(raw.get("inline_link_clicks", 0)), 2)  
    three_sec_views   = get_action_value(actions, "video_view")
    video_100_percent = get_action_value(video_100, "video_p100_watched")

    return {
        "Account_ID": acc_id,
        "Campaign_ID": str(raw.get("campaign_id", "")),
        "Campaign_name": str(raw.get("campaign_name", "")),
        "Ad_ID": str(raw.get("ad_id", "")),
        "Ad_Name": str(raw.get("ad_name", "")),
        "Adset_ID": str(raw.get("adset_id", "")),
        "Adset_Name": str(raw.get("adset_name", "")),
        "Clicks_all":   str(round(safe_float(raw.get("clicks")), 2)),          
        "Link_Clicks":  str(link_clicks),                                        
        "amount_spent": str(round(safe_float(raw.get("spend")), 2)),            
        "Impressions":  str(round(safe_float(raw.get("impressions")), 2)),      
        "Video_plays": str(sum(int(a.get("value",0)) for a in video_play if a.get("action_type")=="video_view")),
        "Video_plays_at_100_percent": str(video_100_percent),
        "Three_second_video_plays": str(three_sec_views),
        "Video_average_play_time": str(round(sum(float(a.get("value",0)) for a in video_avg) / 1000.0, 2)) if video_avg else "0",  
        "Purchases": str(purchases),
        "Purchases_conversion_value": str(purchase_revenue),
        "Initiated_Checkout": str(initiated_checkout),
        "Initiated_Checkout_Value": str(initiated_checkout_value),
        "Add_to_Cart": str(add_to_cart),
        "Add_to_Cart_Value": str(add_to_cart_value),
        "Creative_Facebook_URL": "",
        "Country": "",
        "Currency": "",
        "Creative_thumbnail_URL": "",
        "Adset_creation_time": None,
        "day": safe_date(raw.get("date_start")),
    }

# =============================================
# RETRY DECORATOR
# =============================================
def retry(func):
    def wrapper(*args, **kwargs):
        for attempt in range(1, 6):
            try:
                return func(*args, **kwargs)
            except FacebookRequestError as e:
                if e.api_error_code() in {1,2,4,17,32,190,613} and attempt < 5:
                    time.sleep(min(2 ** attempt, 60))
                    continue
                raise
            except Exception:
                if attempt == 5: raise
                time.sleep(2 * attempt)
        return None
    return wrapper

@retry
def get_insights(account: AdAccount, acc_id: str, since: str, until: str) -> List[Dict]:
    rows = []
    seen = set()
    params = {
        "time_range": {"since": since, "until": until},
        "level": "ad",
        "time_increment": 1,
        "limit": 500,
    }
    cursor = account.get_insights(fields=FIELDS, params=params)
    while True:
        try:
            for item in cursor:
                row = process_row(dict(item), acc_id)
                key = f"{row['Ad_ID']}_{row['day']}"
                if key in seen: continue
                seen.add(key)
                rows.append(row)
            if not cursor.load_next_page(): break
        except StopIteration: break
        except Exception as e:
            logger.error(f"[{acc_id}] Page error: {e}")
            break
    return rows

def fetch_complete(acc_id: str, start: str, end: str) -> List[Dict]:
    FacebookAdsApi.init(FACEBOOK_APP_ID, FACEBOOK_APP_SECRET, FACEBOOK_ACCESS_TOKEN)
    account = AdAccount(f"act_{acc_id}")
    logger.info(f"[{acc_id}] Fetching {start} → {end}")
    data = get_insights(account, acc_id, start, end)

    if data:
        expected = set(get_dates(start, end))
        actual   = {r["day"] for r in data if r["day"]}
        missing  = expected - actual
        if missing:
            logger.info(f"[{acc_id}] Filling {len(missing)} missing dates")
            for d in sorted(missing):
                ds = d.strftime("%Y-%m-%d")
                data.extend(get_insights(account, acc_id, ds, ds))
                time.sleep(0.1)
    logger.info(f"[{acc_id}] Total rows: {len(data)}")
    return data

# =============================================
# CREATIVE ENRICHMENT
# =============================================
async def fetch_one(session, ad_id, token, sem):
    async with sem:
        url    = f"https://graph.facebook.com/v21.0/{ad_id}"
        params = {"fields": "creative{object_story_spec,effective_object_story_id,thumbnail_url,image_url}", "access_token": token}
        try:
            async with session.get(url, params=params, timeout=15) as r:
                if r.status != 200: return ad_id, "", ""
                js = await r.json()
                cr = js.get("creative", {})
                fb_url = thumb = ""
                if isinstance(cr, dict):
                    spec = cr.get("object_story_spec", {})
                    if spec:
                        fb_url = spec.get("link_data", {}).get("link") or spec.get("video_data", {}).get("link") or ""
                    if not fb_url and cr.get("effective_object_story_id"):
                        fb_url = f"https://www.facebook.com/{cr['effective_object_story_id']}"
                    thumb = cr.get("thumbnail_url") or cr.get("image_url") or ""
                return ad_id, fb_url, thumb
        except:
            return ad_id, "", ""

async def enrich_creatives(rows: List[Dict], acc_id: str):
    ad_ids = list({r["Ad_ID"] for r in rows if r["Ad_ID"]})
    if not ad_ids: return
    logger.info(f"[{acc_id}] Enriching {len(ad_ids)} creatives...")
    sem = asyncio.Semaphore(CREATIVE_WORKERS)
    async with aiohttp.ClientSession() as sess:
        tasks   = [fetch_one(sess, aid, FACEBOOK_ACCESS_TOKEN, sem) for aid in ad_ids]
        results = await asyncio.gather(*tasks)
    mapping = {r[0]: (r[1], r[2]) for r in results}
    for row in rows:
        if row["Ad_ID"] in mapping:
            row["Creative_Facebook_URL"], row["Creative_thumbnail_URL"] = mapping[row["Ad_ID"]]

# =============================================
# SAVE TO PARQUET
# =============================================
def save_parquet(rows: List[Dict], path: str, acc_id: str):
    table = pa.table({
        "Account_ID":                 [r["Account_ID"] for r in rows],
        "Campaign_ID":                [r["Campaign_ID"] for r in rows],
        "Campaign_name":              [r["Campaign_name"] for r in rows],
        "Ad_ID":                      [r["Ad_ID"] for r in rows],
        "Ad_Name":                    [r["Ad_Name"] for r in rows],
        "Adset_ID":                   [r["Adset_ID"] for r in rows],
        "Adset_Name":                 [r["Adset_Name"] for r in rows],
        "Clicks_all":                 [r["Clicks_all"] for r in rows],
        "Link_Clicks":                [r["Link_Clicks"] for r in rows],
        "amount_spent":               [r["amount_spent"] for r in rows],
        "Impressions":                [r["Impressions"] for r in rows],
        "Video_plays":                [r["Video_plays"] for r in rows],
        "Video_plays_at_100_percent": [r["Video_plays_at_100_percent"] for r in rows],
        "Three_second_video_plays":   [r["Three_second_video_plays"] for r in rows],
        "Video_average_play_time":    [r["Video_average_play_time"] for r in rows],
        "Purchases":                  [r["Purchases"] for r in rows],
        "Purchases_conversion_value": [r["Purchases_conversion_value"] for r in rows],
        "Initiated_Checkout":         [r["Initiated_Checkout"] for r in rows],
        "Initiated_Checkout_Value":   [r["Initiated_Checkout_Value"] for r in rows],
        "Add_to_Cart":                [r["Add_to_Cart"] for r in rows],
        "Add_to_Cart_Value":          [r["Add_to_Cart_Value"] for r in rows],
        "Creative_Facebook_URL":      [r["Creative_Facebook_URL"] for r in rows],
        "Country":                    [r["Country"] for r in rows],
        "Currency":                   [r["Currency"] for r in rows],
        "Creative_thumbnail_URL":     [r["Creative_thumbnail_URL"] for r in rows],
        "Adset_creation_time":        [r["Adset_creation_time"] for r in rows],
        "day":                        [r["day"] for r in rows],
    })
    pq.write_table(table, path, compression="zstd")
    logger.info(f"[{acc_id}] Saved → {path}")

# =============================================
# MAIN
# =============================================
def run_one_account(acc_id: str, start: str, end: str):
    rows = fetch_complete(acc_id, start, end)
    if not rows:
        raise ValueError("No data")
    asyncio.run(enrich_creatives(rows, acc_id))
    path = os.path.join(RAW_DIR, f"fb_{acc_id}_{start}_{end}.parquet")
    save_parquet(rows, path, acc_id)
    return path

def main():
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    START = '2026-03-22'
    END   = '2026-03-22'

    paths = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(run_one_account, aid, START, END): aid for aid in AD_ACCOUNTS}
        for f in as_completed(futures):
            aid = futures[f]
            try:
                p = f.result()
                paths.append((aid, p))
                logger.info(f"[{aid}] SUCCESS")
            except Exception as e:
                logger.error(f"[{aid}] FAILED → {e}")
                raise

    # ── BigQuery upload using inline credentials ──────────────────────────
    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client   = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    cfg      = bigquery.LoadJobConfig(source_format="PARQUET", write_disposition="WRITE_APPEND")

    for aid, p in paths:
        with open(p, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=cfg)
            job.result()
        logger.info(f"[{aid}] Uploaded to BQ")

    logger.info("ALL DONE!")

if __name__ == "__main__":
    main()
