-- Role utilisé : TRANSFORMER (lecture sur RAW, écriture en dev)
-- Warehouse : tel_insights_etl
-- Ce modèle prend en entrée les trois zones et les fusionne avant nettoyage

WITH base AS (
  SELECT
    TO_TIMESTAMP(event_starttime) AS event_time,
    TRIM(event_imsi) AS imsi,
    TRIM(event_msisdn) AS msisdn,
    TRIM(event_imei) AS imei,
    TRIM(adresse1_numero) || ' ' || TRIM(adresse2_voielieu) || ' ' || TRIM(adresse3_codepostal) || ' ' || TRIM(adresse4_ville) AS address,
    TRIM(adresse1_numero) AS number,
    TRIM(adresse2_voielieu) AS street,
    TRIM(adresse3_codepostal) AS zip_code,
    TRIM(adresse4_ville) AS city,
    UPPER(TRIM(event_libl_fr)) AS event_label,
    UPPER(TRIM(event_country)) AS country,
    CAST(cell_w84x AS FLOAT) AS coord_x,
    CAST(cell_w84y AS FLOAT) AS coord_y,
    UPPER(TRIM(mobile_manufacturer)) AS manufacturer,
    UPPER(TRIM(mobile_type)) AS mobile_type
  FROM {{ source('tel_insights_raw', 'bouygues_events') }}   -- Référence DBT à la table source RAW
  WHERE event_imsi IS NOT NULL
    AND event_code NOT IN ('', 'NULL')
)

SELECT * FROM base
