SELECT
  Z_herkunft_url,
  event_date,
  Z_host,
  herkunft_artikel,
  Z_article_id,
  user_type,
  product_name,
  payment_method,
  Z_ressort,
  Z_knoten_1,
  Z_knoten_2,
  Z_utm_channel_source,
  Z_utm_channel_medium,
  Z_utm_channel_campaign,
  Z_utm_channel_content,
  Z_utm_channel_term,
  Z_author,
  Z_publishtime,
  Z_vrm_premiumarticle
FROM (
  SELECT
    unique_key,
    MAX(Z_herkunft_url) AS Z_herkunft_url,
    MAX(event_date) AS event_date,
    MAX(Z_host) AS Z_host,
    MAX(herkunft_artikel) AS herkunft_artikel,
    MAX(Z_article_id) AS Z_article_id,
    MAX(user_type) AS user_type,
    MAX(product_name) AS product_name,
    MAX(payment_method) AS payment_method,
    MAX(Z_ressort) AS Z_ressort,
    MAX(Z_knoten_1) AS Z_knoten_1,
    MAX(Z_knoten_2) AS Z_knoten_2,
    MAX(Z_utm_channel_source) AS Z_utm_channel_source,
    MAX(Z_utm_channel_medium) AS Z_utm_channel_medium,
    MAX(Z_utm_channel_campaign) AS Z_utm_channel_campaign,
    MAX(Z_utm_channel_content) AS Z_utm_channel_content,
    MAX(Z_utm_channel_term) AS Z_utm_channel_term,
    MAX(Z_author) AS Z_author,
    MAX(Z_publishtime) AS Z_publishtime,
    MAX(Z_vrm_premiumarticle) AS Z_vrm_premiumarticle
  FROM (
    SELECT
      unique_key,
      Z_herkunft_url,
      event_date,
      Z_host,
      herkunft_artikel,
      Z_article_id,
      user_type,
      product_name,
      payment_method,
      Z_ressort,
      Z_knoten_1,
      Z_knoten_2,
      Z_utm_channel_source,
      Z_utm_channel_medium,
      Z_utm_channel_campaign,
      Z_utm_channel_content,
      Z_utm_channel_term,
      Z_author,
      Z_publishtime,
      Z_vrm_premiumarticle
    FROM (
      SELECT
        DISTINCT *
      FROM (
        SELECT
          sha1(CONCAT(SAFE_CAST(DATE(visit_date) AS string), herkunft_artikel, cx_user_id)) AS unique_key,
          cx_user_id,
          visit_date AS event_date,
          CONCAT(REGEXP_REPLACE(herkunft_hostname, '/api/auth', ''), herkunft_artikel) AS Z_herkunft_url,
          CASE
            WHEN herkunft_hostname LIKE "%allgemeine-zeitung.de%" THEN "Allgemeine Zeitung"
            WHEN herkunft_hostname LIKE "%buerstaedter-zeitung.de%"
          OR herkunft_hostname LIKE "%lampertheimer-zeitung.de%" THEN "Bürstädter Zeitung + Lampertheimer Zeitung"
            WHEN herkunft_hostname LIKE "%echo-online.de%" THEN "Echo Online"
            WHEN herkunft_hostname LIKE "%giessener-anzeiger.de%" THEN "Giessener Anzeiger"
            WHEN herkunft_hostname LIKE "%hochheimer-zeitung.de%" THEN "Hochheimer Zeitung"
            WHEN herkunft_hostname LIKE "%hofheimer-zeitung.de%" THEN "Hofheimer Zeitung"
            WHEN herkunft_hostname LIKE "%kreis-anzeiger.de%" THEN "Kreis Anzeiger"
            WHEN herkunft_hostname LIKE "%lauterbacher-anzeiger.de%"
          OR herkunft_hostname LIKE "%oberhessische-zeitung.de%" THEN "Lauterbacher Anzeiger + Oberhessische Zeitung"
            WHEN herkunft_hostname LIKE "%main-spitze.de%" THEN "Main Spitze"
            WHEN herkunft_hostname LIKE "%mittelhessen.de%" THEN "Mittelhessen"
            WHEN herkunft_hostname LIKE "%usinger-anzeiger.de%" THEN "Usinger Anzeiger"
            WHEN herkunft_hostname LIKE "%wiesbadener-kurier.de%" THEN "Wiesbadener Kurier"
            WHEN herkunft_hostname LIKE "%wormser-zeitung.de%" THEN "Wormser Zeitung"
          ELSE
          NULL
        END
          AS Z_host,
          herkunft_artikel,
          SAFE_CAST(REGEXP_EXTRACT(herkunft_artikel, "[^_]+$") AS int64) AS Z_article_id,
          user_type,
          product_name,
          payment_method,
          REGEXP_EXTRACT(herkunft_artikel, '/([^/]+)/') AS Z_ressort,
          REGEXP_EXTRACT(herkunft_artikel, '[^/]+/([^/]+)/') AS Z_knoten_1,
          REGEXP_EXTRACT(herkunft_artikel, '[^/]+/[^/]+/([^/]+)/') AS Z_knoten_2,
        IF
          (REGEXP_EXTRACT(utm_channel, '"utm_source":"(.*?)"') = '',
            NULL,
            REGEXP_EXTRACT(utm_channel, '"utm_source":"(.*?)"')) AS Z_utm_channel_source,
        IF
          (REGEXP_EXTRACT(utm_channel, '"utm_medium":"(.*?)"') = '',
            NULL,
            REGEXP_EXTRACT(utm_channel, '"utm_medium":"(.*?)"')) AS Z_utm_channel_medium,
        IF
          (REGEXP_EXTRACT(utm_channel, '"utm_campaign":"(.*?)"') ='',
            NULL,
            REGEXP_EXTRACT(utm_channel, '"utm_campaign":"(.*?)"')) AS Z_utm_channel_campaign,
        IF
          (REGEXP_EXTRACT(utm_channel, '"utm_content":"(.*?)"') ='',
            NULL,
            REGEXP_EXTRACT(utm_channel, '"utm_content":"(.*?)"')) AS Z_utm_channel_content,
        IF
          (REGEXP_EXTRACT(utm_channel, '"utm_term":"(.*?)"') ='',
            NULL,
            REGEXP_EXTRACT(utm_channel, '"utm_term":"(.*?)"')) AS Z_utm_channel_term,
        FROM
          {{source('piano', 'bestellprozess')}}
        WHERE
          payment_method != ''
          AND herkunft_artikel IS NOT NULL
          AND NOT REGEXP_CONTAINS(herkunft_hostname, 'staging')
          AND NOT REGEXP_CONTAINS(herkunft_hostname, 'stories')
          AND NOT REGEXP_CONTAINS(herkunft_hostname, 'localhost')
          AND utm_channel NOT LIKE '%test%' ) AS conversions
      LEFT JOIN (
        SELECT
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
          article_id AS Z_article_id,
          MAX(author) AS Z_author,
          MIN(publishtime) AS Z_publishtime,
          MAX(vrm_premiumarticle) AS Z_vrm_premiumarticle
        FROM
          {{source('piano', 'content_profile')}}
        GROUP BY
          Z_host,
          Z_article_id) AS content
      USING
        (Z_host,
          Z_article_id) ))
  GROUP BY
    unique_key)