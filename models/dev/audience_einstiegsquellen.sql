{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'visit_date', 'data_type': 'date'},
    partitions = partitions_to_replace
  )
}}

SELECT
  CASE
    WHEN bundle_text = 'PLUS' OR REGEXP_CONTAINS(bundle_text, ';PLUS;') OR REGEXP_CONTAINS(bundle_text, ';WEB & APP;') OR (REGEXP_CONTAINS(bundle_text, 'PLUS;') AND NOT REGEXP_CONTAINS(bundle_text, ' PLUS;')) OR (REGEXP_CONTAINS(bundle_text, ';PLUS') AND NOT REGEXP_CONTAINS(bundle_text, 'AKTIONSABO')) OR bundle_text = 'PLUS AKTIONSABO;PLUS' OR bundle_text = 'WEB & APP' OR (REGEXP_CONTAINS(bundle_text, 'WEB & APP;') AND NOT REGEXP_CONTAINS(bundle_text, ' WEB & APP;')) OR (REGEXP_CONTAINS(bundle_text, ';WEB & APP') AND NOT REGEXP_CONTAINS(bundle_text, 'AKTIONSABO')) THEN 'PLUS'
    WHEN bundle_text IN ('PLUS AKTIONSABO',
    'WEB & APP AKTIONSABO')OR REGEXP_CONTAINS(bundle_text, ';PLUS AKTIONSABO;')OR REGEXP_CONTAINS(bundle_text, 'PLUS AKTIONSABO;')
  OR REGEXP_CONTAINS(bundle_text, ';PLUS AKTIONSABO')
  OR REGEXP_CONTAINS(bundle_text, ';WEB & APP AKTIONSABO;')
  OR REGEXP_CONTAINS(bundle_text, 'WEB & APP AKTIONSABO;')
  OR REGEXP_CONTAINS(bundle_text, ';WEB & APP AKTIONSABO') THEN 'PLUS AKTIONSABO'
    WHEN bundle_text = 'PLUS PREMIUM' OR REGEXP_CONTAINS(bundle_text, ';PLUS PREMIUM;') OR REGEXP_CONTAINS(bundle_text, 'PLUS PREMIUM;') OR REGEXP_CONTAINS(bundle_text, ';PLUS PREMIUM') THEN 'PLUS PREMIUM'
    WHEN bundle_text = 'unknown'
  OR bundle_text IS NULL
  OR bundle_text = '' THEN 'ausgeloggt/kein Bündel/AMP'
  ELSE
  'andere Abonnenten'
END
  AS bundle_text_dim_new,
  COUNT(*) AS pageviews_agg,
  CASE
    WHEN REGEXP_CONTAINS(referrer_host, 'allgemeine') AND REGEXP_CONTAINS(referrer_host, 'zeitung') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'buerstaedter')
  AND REGEXP_CONTAINS(referrer_host, 'zeitung')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'echo') AND REGEXP_CONTAINS(referrer_host, 'online') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'giessener')
  AND REGEXP_CONTAINS(referrer_host, 'anzeiger')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'hochheimer') AND REGEXP_CONTAINS(referrer_host, 'zeitung') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'hofheimer')
  AND REGEXP_CONTAINS(referrer_host, 'zeitung')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'kreis') AND REGEXP_CONTAINS(referrer_host, 'anzeiger') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'lampertheimer')
  AND REGEXP_CONTAINS(referrer_host, 'zeitung')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'lauterbacher') AND REGEXP_CONTAINS(referrer_host, 'zeitung') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'main')
  AND REGEXP_CONTAINS(referrer_host, 'spitze')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'mittelhessen') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'oberhessische')
  AND REGEXP_CONTAINS(referrer_host, 'zeitung')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'usinger') AND REGEXP_CONTAINS(referrer_host, 'zeitung') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'wiesbadener')
  AND REGEXP_CONTAINS(referrer_host, 'kurier')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'wormser') AND REGEXP_CONTAINS(referrer_host, 'zeitung') AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'vrm')
  AND utm_source IS NULL THEN 'internal'
    WHEN REGEXP_CONTAINS(referrer_host, 'google') AND utm_source IS NULL THEN 'google'
    WHEN REGEXP_CONTAINS(LOWER(utm_source), 'google')
  AND REGEXP_CONTAINS(utm_medium, 'paid') THEN 'google paid'
    WHEN REGEXP_CONTAINS(referrer_host, 'facebook') AND utm_source IS NULL THEN 'facebook'
    WHEN REGEXP_CONTAINS(LOWER(utm_source), 'facebook')
  AND REGEXP_CONTAINS(utm_medium, 'paid') THEN 'facebook paid'
    WHEN REGEXP_CONTAINS(referrer_host, 'insta') AND utm_source IS NULL THEN 'instagram'
    WHEN (REGEXP_CONTAINS(referrer_host, 'newsletter')
    AND utm_source IS NULL)
  OR REGEXP_CONTAINS(utm_source, 'newsletter') THEN 'newsletter'
    WHEN REGEXP_CONTAINS(referrer_host, 'twitter') AND utm_source IS NULL THEN 'twitter'
    WHEN REGEXP_CONTAINS(referrer_host, 'youtube')
  AND utm_source IS NULL THEN 'youtube'
    WHEN REGEXP_CONTAINS(referrer_host, 'display') AND utm_source IS NULL THEN 'display'
    WHEN (REGEXP_CONTAINS(referrer_host, 'push')
    AND utm_source IS NULL)
  OR (REGEXP_CONTAINS(referrer_host, 'notification')
    AND utm_source IS NULL)
  OR REGEXP_CONTAINS(utm_source, 'notification') THEN 'push'
    WHEN REGEXP_CONTAINS(referrer_host, 'ecosia') THEN 'search'
    WHEN utm_source IS NOT NULL THEN 'other'
  ELSE
  LOWER(referrer_host_class)
END
  AS refer_host_new,
  REGEXP_EXTRACT(Z_url, '^https://[^/]+[^/]+/[^/]+/([^/]+)/') AS regex_knoten1,
  REGEXP_EXTRACT(Z_url, '^https://[^/]+[^/]+/[^/]+/[^/]+/([^/]+)/') AS regex_knoten2,
  REGEXP_EXTRACT(Z_url, '^https://[^/]+/([^/]+)/') AS regex_ressort,
  coalesce(REGEXP_EXTRACT(Z_url, '^https://[^/]+/(sport|panorama|politik|wirtschaft|ratgeber|freizeit|kultur|lokales+)/'),
    'sonstiges') AS ressorts,
IF
  (article_id IS NULL,
    1,
    2) AS seitentyp_calc,
  datetime_TRUNC(visit_date,
    hour) AS visit_date,
  Z_host,
FROM
  {{ ref('visits_content')}}
WHERE
  session_start
  
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND visit_date >= datetime_sub(current_datetime, interval 8 hour)

{% endif %}

GROUP BY
  bundle_text,
  refer_host_new,
  regex_knoten1,
  regex_knoten2,
  regex_ressort,
  ressorts,
  seitentyp_calc,
  visit_date,
  Z_host