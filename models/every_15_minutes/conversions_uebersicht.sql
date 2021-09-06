SELECT
  *EXCEPT(unique_key),
  COUNT(*) AS conversion_agg
FROM (
  SELECT
    unique_key,
    MAX(visit_date) AS visit_date,
    MAX(Z_host) AS Z_host,
    MAX(CASE
        WHEN herkunft_artikel IN ('/&cxrecs_s', '/') THEN 'Marketing'
        WHEN REGEXP_CONTAINS(Z_utm_channel_source, 'paid')
      OR REGEXP_CONTAINS(Z_utm_channel_medium, 'paid') THEN 'Marketing'
        WHEN REGEXP_CONTAINS(Z_utm_channel_source, 'floorad') OR REGEXP_CONTAINS(Z_utm_channel_medium, 'floorad') THEN 'Marketing'
      ELSE
      'Artikelconversion'
    END
      ) AS herkunft_kategorie,
    MAX(Z_ressort) AS Z_ressort,
    MAX(Z_knoten_1) AS Z_knoten_1,
    MAX(Z_knoten_2) AS Z_knoten_2,
    MAX(user_type) AS user_type,
    MAX(product_name) AS product_name,
    MAX(payment_method) AS payment_method
  FROM (
    SELECT
      sha1(CONCAT(SAFE_CAST(DATE(visit_date) AS string), herkunft_artikel, cx_user_id)) AS unique_key,
      visit_date,
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
        REGEXP_EXTRACT(utm_channel, '"utm_medium":"(.*?)"')) AS Z_utm_channel_medium
    FROM
      {{ source('piano', 'bestellprozess') }}
    WHERE
      payment_method != ''
      AND herkunft_artikel IS NOT NULL
      AND NOT REGEXP_CONTAINS(herkunft_hostname, 'staging')
      AND NOT REGEXP_CONTAINS(herkunft_hostname, 'stories')
      AND NOT REGEXP_CONTAINS(herkunft_hostname, 'localhost')
      AND utm_channel NOT LIKE '%testbuchung%' )
  GROUP BY
    unique_key)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9