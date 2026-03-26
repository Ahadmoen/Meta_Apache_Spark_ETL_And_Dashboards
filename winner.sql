WITH

-- ── 1. Deduplicate creative URLs per Ad_Name ────────────────────────────────
url_dedup AS (
  SELECT
    Ad_Name,
    MAX(Creative_Facebook_URL)  AS Creative_Facebook_URL,
    MAX(Creative_thumbnail_URL) AS Creative_thumbnail_URL
  FROM `growthruben.Meta.Facebook_Ads`
  GROUP BY Ad_Name
),

-- ── 2. Daily rows: cast + parse naming convention ───────────────────────────
daily AS (
  SELECT
    f.Ad_Name,
    f.day,
    DATE_TRUNC(f.day, WEEK(MONDAY)) AS week_start,

    -- Numeric casts
    SAFE_CAST(f.amount_spent               AS FLOAT64) AS amount_spent,
    SAFE_CAST(f.Purchases_conversion_value AS FLOAT64) AS revenue,
    SAFE_CAST(f.Purchases                  AS FLOAT64) AS purchases,
    SAFE_CAST(f.Impressions                AS FLOAT64) AS impressions,
    SAFE_CAST(f.Clicks_all                 AS FLOAT64) AS clicks_all,
    SAFE_CAST(f.Link_Clicks                AS FLOAT64) AS link_clicks,
    SAFE_CAST(f.Add_to_Cart                AS FLOAT64) AS add_to_cart,
    SAFE_CAST(f.Add_to_Cart_Value          AS FLOAT64) AS add_to_cart_value,
    SAFE_CAST(f.Initiated_Checkout         AS FLOAT64) AS initiated_checkout,
    SAFE_CAST(f.Initiated_Checkout_Value   AS FLOAT64) AS initiated_checkout_value,
    SAFE_CAST(f.Video_plays                AS FLOAT64) AS video_plays,
    SAFE_CAST(f.Video_plays_at_100_percent AS FLOAT64) AS video_plays_100pct,
    SAFE_CAST(f.Three_second_video_plays   AS FLOAT64) AS video_plays_3s,
    SAFE_CAST(f.Video_average_play_time    AS FLOAT64) AS video_avg_play_time,

    -- Fixed Facebook permalink
    CASE
      WHEN u.Creative_Facebook_URL IS NOT NULL
        AND REGEXP_CONTAINS(u.Creative_Facebook_URL, r'/(\d+)_(\d+)')
      THEN CONCAT(
        'https://www.facebook.com/',
        REGEXP_EXTRACT(u.Creative_Facebook_URL, r'/(\d+)_'),
        '/posts/',
        REGEXP_EXTRACT(u.Creative_Facebook_URL, r'_(\d+)$')
      )
      ELSE u.Creative_Facebook_URL
    END AS creative_facebook_url,
    u.Creative_thumbnail_URL AS creative_thumbnail_url,

    -- Parsed naming convention (separator = ' _ ')
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(0)]                               AS awareness,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(1)]                               AS campaign_id,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(2)]                               AS creative_id,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(3)]                               AS creative_variation,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(4)]                               AS creative_strategist,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(5)]                               AS creative_type,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(6)]                               AS brief_type,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(7)]                               AS angle,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(8)]                               AS product,
    SAFE.PARSE_DATE('%Y-%m-%d', SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(9)])  AS ad_launch_date,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(10)]                              AS market,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(11)]                              AS editor,
    SPLIT(f.Ad_Name, ' _ ')[SAFE_OFFSET(12)]                              AS cta

  FROM `growthruben.Meta.Facebook_Ads` f
  LEFT JOIN url_dedup u USING (Ad_Name)
),

-- ── 3. Lifetime totals per Ad_Name (winner threshold evaluated here) ────────
lifetime AS (
  SELECT
    Ad_Name,
    SUM(amount_spent)                                AS lifetime_spend,
    SUM(revenue)                                     AS lifetime_revenue,
    SUM(purchases)                                   AS lifetime_purchases,
    SAFE_DIVIDE(SUM(revenue), SUM(amount_spent))     AS lifetime_roas,
    SAFE_DIVIDE(SUM(amount_spent), SUM(purchases))   AS lifetime_cpa
  FROM daily
  GROUP BY Ad_Name
),

-- ── 4. First launch date per creative_id (used for Revived logic) ───────────
first_launch AS (
  SELECT
    creative_id,
    MIN(ad_launch_date) AS first_launch_date
  FROM daily
  WHERE creative_id IS NOT NULL
  GROUP BY creative_id
)

-- ── 5. Final view: daily grain + enrichment columns ─────────────────────────
SELECT
  d.*,

  -- Launch week (from ad naming, not from day — for "creatives launched per week")
  DATE_TRUNC(d.ad_launch_date, WEEK(MONDAY)) AS launch_week,

  -- ── Lifetime performance ──────────────────────────────────────────────────
  l.lifetime_spend,
  l.lifetime_revenue,
  l.lifetime_purchases,
  l.lifetime_roas,
  l.lifetime_cpa,

  -- ── Winner flag (spend ≥ 1000 AND ROAS ≥ 2.0) ────────────────────────────
  CASE
    WHEN l.lifetime_spend >= 1000 AND l.lifetime_roas >= 2.0 THEN TRUE
    ELSE FALSE
  END AS is_winner,

  -- ── Creative label (New / Variation / Revived) ────────────────────────────
  --    Variation  → creative_variation contains a dot  (e.g. I02.1)
  --    Revived    → same creative_id re-launched after its first seen date
  --    New        → everything else
  CASE
    WHEN REGEXP_CONTAINS(d.creative_variation, r'\.\d+')  THEN 'Variation'
    WHEN d.ad_launch_date > fl.first_launch_date           THEN 'Revived Creative'
    ELSE                                                        'New Creative'
  END AS creative_label

FROM daily d
LEFT JOIN lifetime     l  USING (Ad_Name)
LEFT JOIN first_launch fl ON d.creative_id = fl.creative_id
