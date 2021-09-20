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

WITH
  basic_cs AS (
  WITH
    visits_content_sessions AS (
    SELECT
      DISTINCT *
    FROM (
      SELECT
        cx_user_id,
        event_id,
        user_id,
        cx_channel,
        visit_date,
        article_id,
        bundle_text,
        is_logged_in,
        is_subscriber,
        referrer_social_network,
        session_start,
        active_time,
        article_tags,
        Z_host,
        Z_url,
        Z_publishtime,
        Z_vrm_premiumarticle,
        paid_content,
        REGEXP_EXTRACT(Z_url, '[^/]+/([^/]+)/') AS Z_ressort,
        REGEXP_EXTRACT(Z_url, '[^/]+/[^/]+/([^/]+)/') AS Z_knoten_1,
        REGEXP_EXTRACT(Z_url, '[^/]+/[^/]+/[^/]+/([^/]+)/') AS Z_knoten_2
      FROM
        {{ref('visits_content')}}
      WHERE
        article_id IS NOT NULL
        AND Z_host NOT IN ('Hochheimer Zeitung',
          'Hofheimer Zeitung',
          'sonst?') ) AS v
    LEFT JOIN (
      SELECT
        site_id,
        visit_date,
        event_id,
        cx_user_id,
        session_bounce,
        session_stop
      FROM
        {{source('piano', 'session_data')}}) AS s
    USING
      (visit_date,
        event_id,
        cx_user_id) ),
    visits_content_sessions_events AS (
    SELECT
      DISTINCT * EXCEPT(cx_user_id,
        event_date,
        page_view_event_id,
        artikelende_erreicht,
        social_button,
        site_id),
      coalesce(artikelende_erreicht,
        0) AS artikelende_erreicht,
      coalesce(social_button,
        0) AS social_button
    FROM
      visits_content_sessions AS vcs
    LEFT JOIN (
      SELECT
        DISTINCT cx_user_id,
        DATE(event_date) AS event_date,
        site_id,
        page_view_event_id,
        COUNTIF(custom_value_1 = 'Artikelende erreicht') AS artikelende_erreicht,
        COUNTIF(custom_group_1 = 'social_button') AS social_button
      FROM
        {{source('piano', 'events')}}
      WHERE
        custom_value_1 = 'Artikelende erreicht'
        OR custom_group_1 = 'social_button'
      GROUP BY
        cx_user_id,
        event_date,
        site_id,
        page_view_event_id) AS e
    ON
      vcs.event_id = e.page_view_event_id
      AND vcs.cx_user_id = e.cx_user_id
      AND DATE(vcs.visit_date) = e.event_date
      AND e.site_id = vcs.site_id )
    -- Start select statement
  SELECT
    visit_date,
    event_id,
    cx_channel,
    article_id,
    bundle_text,
    is_logged_in,
    is_subscriber,
    referrer_social_network,
    session_start,
    active_time,
    article_tags,
    Z_host,
    Z_url,
    Z_publishtime,
    Z_vrm_premiumarticle,
    paid_content,
    Z_ressort,
    Z_knoten_1,
    Z_knoten_2,
    session_bounce,
    session_stop,
    artikelende_erreicht,
    social_button,
    coalesce(conversions,
      0) AS conversions,
    sum_facebook
  FROM
    visits_content_sessions_events AS vcse
  LEFT JOIN (
    SELECT
      Z_herkunft_url,
      DATE(event_date) AS date,
      COUNT(*) AS conversions
    FROM
      {{ref('conversions')}}
    GROUP BY
      Z_herkunft_url,
      DATE(event_date)) AS c
  ON
    Z_url = Z_herkunft_url
    AND date = DATE(visit_date)
  LEFT JOIN (
    SELECT
      Z_host,
      article_id,
      SUM(sum_max_values) AS sum_facebook
    FROM (
      SELECT
        article_id,
        Z_host,
        page,
        MAX(shares) + MAX(comments) + MAX(reactions) AS sum_max_values
      FROM (
        SELECT
          SAFE_CAST(REGEXP_EXTRACT(url, '_([0-9]*)$') AS int64) AS article_id,
          CASE
            WHEN url LIKE "%allgemeine-zeitung.de%" THEN "Allgemeine Zeitung"
            WHEN url LIKE "%buerstaedter-zeitung.de%"
          OR url LIKE "%lampertheimer-zeitung.de%" THEN "Bürstädter Zeitung + Lampertheimer Zeitung"
            WHEN url LIKE "%echo-online.de%" THEN "Echo Online"
            WHEN url LIKE "%giessener-anzeiger.de%" THEN "Giessener Anzeiger"
            WHEN url LIKE "%hochheimer-zeitung.de%" THEN "Hochheimer Zeitung"
            WHEN url LIKE "%hofheimer-zeitung.de%" THEN "Hofheimer Zeitung"
            WHEN url LIKE "%kreis-anzeiger.de%" THEN "Kreis Anzeiger"
            WHEN url LIKE "%lauterbacher-anzeiger.de%"
          OR url LIKE "%oberhessische-zeitung.de%" THEN "Lauterbacher Anzeiger + Oberhessische Zeitung"
            WHEN url LIKE "%main-spitze.de%" THEN "Main Spitze"
            WHEN url LIKE "%mittelhessen.de%" THEN "Mittelhessen"
            WHEN url LIKE "%usinger-anzeiger.de%" THEN "Usinger Anzeiger"
            WHEN url LIKE "%wiesbadener-kurier.de%" THEN "Wiesbadener Kurier"
            WHEN url LIKE "%wormser-zeitung.de%" THEN "Wormser Zeitung"
          ELSE
          NULL
        END
          AS Z_host,
          page,
          shares,
          comments,
          reactions
        FROM
          {{source('piano', 'facebook_data')}} )
      GROUP BY
        page,
        Z_host,
        article_id)
    GROUP BY
      article_id,
      Z_host)
  USING
    (article_id,
      Z_host))
SELECT
  *
FROM (
  SELECT
  IF
    (paid_content IS NULL
      AND cx_channel='web',
      active_time,
      NULL) AS active_time_web_no_paid,
    article_id,
  IF
    (paid_content IS NULL
      AND cx_channel = 'web',
      artikelende_erreicht,
      NULL) AS artikelende_erreicht_bereinigt,
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
    conversions,
    cx_channel,
    1 AS pageviews_agg,
  IF
    (cx_channel = 'web',
      1,
      NULL) AS pageviews_web,
  IF
    (paid_content IS NULL
      AND cx_channel = 'web',
      1,
      NULL) AS pageviews_web_und_ohne_paid,
    REGEXP_EXTRACT(Z_url,'/([^/]*)$') AS regex_artikelende,
    coalesce(REGEXP_EXTRACT(Z_url, '^https://[^/]+/(sport|panorama|politik|wirtschaft|ratgeber|freizeit|kultur|lokales+)/'),
      'sonstiges') AS ressorts,
  IF
    (session_bounce,
      1,
      0) AS sessionbounce_val,
  IF
    (session_start,
      1,
      0) AS sessionstart_val,
    social_button,
  IF
    (referrer_social_network IS NULL,
      0,
      1) AS socialnetwork_val,
    sum_facebook,
    visit_date,
    CASE
      WHEN Z_vrm_premiumarticle THEN 'Plus'
      WHEN NOT Z_vrm_premiumarticle THEN 'Free'
    ELSE
    'n.v.'
  END
    AS vrm_premiumarticle_dim,
    Z_host,
    Z_knoten_1,
    Z_knoten_2,
    Z_publishtime,
    Z_url
  FROM
    basic_cs)
WHERE
  ressorts != 'sonstiges'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND visit_date >= datetime_sub(current_datetime, interval 8 hour)

{% endif %}

