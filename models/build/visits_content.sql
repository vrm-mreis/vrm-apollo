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

select * from (
WITH
  visits_modified AS (
  SELECT
    DISTINCT *
  FROM (
      -- step 4: convert app url to normal
    SELECT
      * EXCEPT(Z_tmp_url),
      CASE
        WHEN Z_tmp_url LIKE '%/api/na/eo%' THEN REGEXP_REPLACE(Z_tmp_url, '/api/na/eo', '')
        WHEN Z_tmp_url LIKE '%/api/na/mh%' THEN REGEXP_REPLACE(Z_tmp_url, '/api/na/mh', '')
        WHEN Z_tmp_url LIKE '%/api/na/vrm%' THEN REGEXP_REPLACE(Z_tmp_url, '/api/na/vrm', '')
        WHEN Z_tmp_url LIKE '%/na/eo%' THEN REGEXP_REPLACE(Z_tmp_url, '/na/eo', '')
        WHEN Z_tmp_url LIKE '%/na/mh%' THEN REGEXP_REPLACE(Z_tmp_url, '/na/mh', '')
        WHEN Z_tmp_url LIKE '%/na/vrm%' THEN REGEXP_REPLACE(Z_tmp_url, '/na/vrm', '')
      ELSE
      Z_tmp_url
    END
      AS Z_url
    FROM (
        -- step 3: handle missing domain
      SELECT
        * EXCEPT(Z_tmp_url),
      IF
        (Z_tmp_url LIKE '%.de%',
        IF
          (REGEXP_EXTRACT(Z_tmp_url, '\\.(.*)\\.') IS NULL,
            # no domain but .de
            CASE
              WHEN site_id = '1145195256381560328' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.allgemeine-zeitung')
              WHEN site_id = '1131711053701389814' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.buerstaedter-zeitung')
              WHEN site_id = '1131711053701389815' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.echo-online')
              WHEN site_id = '1131711053701389816' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.giessener-anzeiger')
              WHEN site_id = '1134007183008072068' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.hochheimer-zeitung')
              WHEN site_id = '1135133084573409696' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.hofheimer-zeitung')
              WHEN site_id = '1131711053701389820' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.kreis-anzeiger')
              WHEN site_id = '1131711053701389813' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.lampertheimer-zeitung')
              WHEN site_id = '1131711053701389819' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.lauterbacher-anzeiger')
              WHEN site_id = '1131711053701389812' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.main-spitze')
              WHEN site_id = '1134023502631265061' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.mittelhessen')
              WHEN site_id = '1131711053701389821' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.oberhessische-zeitung')
              WHEN site_id = '1131711053701389818' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.usinger-anzeiger')
              WHEN site_id = '1131711053701389810' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.wiesbadener-kurier')
              WHEN site_id = '1131711053701389809' THEN REGEXP_REPLACE(Z_tmp_url, '//www', '//www.wormser-zeitung')
            ELSE
            'TODO???'
          END
            ,
            Z_tmp_url),
          # no domain
          CASE
            WHEN site_id = '1145195256381560328' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.allgemeine-zeitung.de/')
            WHEN site_id = '1131711053701389814' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.buerstaedter-zeitung.de/')
            WHEN site_id = '1131711053701389815' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.echo-online.de/')
            WHEN site_id = '1131711053701389816' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.giessener-anzeiger.de/')
            WHEN site_id = '1134007183008072068' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.hochheimer-zeitung.de/')
            WHEN site_id = '1135133084573409696' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.hofheimer-zeitung.de/')
            WHEN site_id = '1131711053701389820' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.kreis-anzeiger.de/')
            WHEN site_id = '1131711053701389813' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.lampertheimer-zeitung.de/')
            WHEN site_id = '1131711053701389819' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.lauterbacher-anzeiger.de/')
            WHEN site_id = '1131711053701389812' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.main-spitze.de/')
            WHEN site_id = '1134023502631265061' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.mittelhessen.de/')
            WHEN site_id = '1131711053701389821' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.oberhessische-zeitung.de/')
            WHEN site_id = '1131711053701389818' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.usinger-anzeiger.de/')
            WHEN site_id = '1131711053701389810' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.wiesbadener-kurier.de/')
            WHEN site_id = '1131711053701389809' THEN REGEXP_REPLACE(Z_tmp_url, '//', '//www.wormser-zeitung.de/')
          ELSE
          'TODO???'
        END
          ) AS Z_tmp_url
      FROM (
          -- 2. step: https instead of http
        SELECT
          *EXCEPT(Z_tmp_url),
        IF
          (REGEXP_CONTAINS(REGEXP_EXTRACT(Z_tmp_url, '^(.*?)[.]'), 'https://www'),
            Z_tmp_url,
            REGEXP_REPLACE(Z_tmp_url, '^(.*?)[.]', 'https://www.')) AS Z_tmp_url
        FROM (
            -- 1. step: replace /amp/ with '/' in url
          SELECT
            site_id,
            event_id,
            user_id,
            cx_channel,
            visit_date,
            article_id,
            cx_user_id,
            bundle_text,
            is_logged_in,
            is_subscriber,
            utm_source,
            dossier,
            postal_code,
            referrer_url,
            referrer_host,
            referrer_host_class,
            referrer_social_network,
            device_type,
            session_start,
            active_time,
            publishtime,
            vrm_premiumarticle,
            utm_campaign,
            utm_term,
            utm_medium,
            utm_content,
            article_tags,
            CASE
              WHEN site_id = '1145195256381560328' THEN 'Allgemeine Zeitung'
              WHEN site_id IN ('1131711053701389814',
              '1131711053701389813') THEN 'Bürstädter Zeitung + Lampertheimer Zeitung'
              WHEN site_id = '1131711053701389815' THEN 'Echo Online'
              WHEN site_id = '1131711053701389816' THEN 'Giessener Anzeiger'
              WHEN site_id = '1134007183008072068' THEN 'Hochheimer Zeitung'
              WHEN site_id = '1135133084573409696' THEN 'Hofheimer Zeitung'
              WHEN site_id = '1131711053701389820' THEN 'Kreis Anzeiger'
              WHEN site_id IN ('1131711053701389819',
              '1131711053701389821') THEN 'Lauterbacher Anzeiger + Oberhessische Zeitung'
              WHEN site_id = '1131711053701389812' THEN 'Main Spitze'
              WHEN site_id = '1134023502631265061' THEN 'Mittelhessen'
              WHEN site_id = '1131711053701389818' THEN 'Usinger Anzeiger'
              WHEN site_id = '1131711053701389810' THEN 'Wiesbadener Kurier'
              WHEN site_id = '1131711053701389809' THEN 'Wormser Zeitung'
            ELSE
            'sonst?'
          END
            AS Z_host,
            REGEXP_REPLACE(url, '/amp/', '/') AS Z_tmp_url
          FROM
             {{ source('piano', 'visits') }}
          WHERE
            site_id NOT IN ('1135184684799916681',
              '1138553117665816035',
              '1131711053701389817')
            AND NOT ENDS_WITH(url, '.php')
            AND cx_channel IS NOT NULL
            AND visit_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY))))) AS visits),
  content_data AS (
  SELECT
    *
  FROM (
    SELECT
      DISTINCT site_id,
      article_id,
      vrm_article_tags,
      publishtime,
      vrm_premiumarticle,
      ROW_NUMBER() OVER(PARTITION BY site_id, article_id ORDER BY og_updated_time DESC, publishtime DESC) AS rownum
    FROM
       {{ source('piano', 'content_profile') }})
  WHERE
    rownum = 1)
  --
  -- Start Select
  --
SELECT
  DISTINCT *
FROM (
  SELECT
    DISTINCT event_id,
    user_id,
    cx_channel,
    visit_date,
    visits_mod.article_id,
    cx_user_id,
    bundle_text,
    is_logged_in,
    is_subscriber,
    utm_source,
    dossier,
    postal_code,
    referrer_url,
    referrer_host,
    referrer_host_class,
    referrer_social_network,
    device_type,
    session_start,
    active_time,
    utm_campaign,
    utm_term,
    utm_medium,
    utm_content,
    coalesce(visits_mod.article_tags,
      content.vrm_article_tags) AS article_tags,
    Z_host,
    Z_url,
    coalesce(visits_mod.publishtime,
      content.publishtime) AS Z_publishtime,
    coalesce(visits_mod.vrm_premiumarticle,
      content.vrm_premiumarticle) AS Z_vrm_premiumarticle
  FROM
    visits_modified AS visits_mod
  LEFT JOIN
    content_data AS content
  ON
    (content.site_id = visits_mod.site_id
      AND content.article_id = visits_mod.article_id)) AS vc
LEFT JOIN (
  SELECT
    cx_user_id,
    page_view_event_id AS event_id,
    custom_value_1 AS paid_content
  FROM
   {{ source('piano', 'events') }}
  WHERE
    custom_value_1 IN ('Overlay Plus',
      'Overlay Hyb'))
USING
  (cx_user_id,
    event_id)
)
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where visit_date >= datetime_sub(current_datetime, interval 8 hour)

{% endif %}