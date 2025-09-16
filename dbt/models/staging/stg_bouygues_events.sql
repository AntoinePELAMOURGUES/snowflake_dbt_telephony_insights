-- Role utilisé : TRANSFORMER (lecture sur RAW, écriture en dev)
-- Warehouse : tel_insights_etl
-- Ce modèle prend en entrée les trois zones et les fusionne avant nettoyage

WITH base AS (
  SELECT
    TO_TIMESTAMP(event_starttime) AS Date,
    TRIM(event_imsi) AS imsi,
    TRIM(event_msisdn) AS msisdn,
    TRIM(event_imei) AS imei,
    UPPER(TRIM(adresse1_numero) || ' ' || TRIM(adresse2_voielieu) || ' ' || TRIM(adresse3_codepostal) || ' ' || TRIM(adresse4_ville)) AS adresse,
    TRIM(adresse1_numero) AS numero,
    TRIM(adresse2_voielieu) AS voie,
    TRIM(adresse3_codepostal) AS code_postal,
    TRIM(adresse4_ville) AS ville,
    UPPER(TRIM(event_libl_fr)) AS evenement,
    UPPER(TRIM(event_country)) AS pays,
    TRY_TO_DOUBLE(REPLACE(Cell_W84X, ',', '.')) AS latitude,
    TRY_TO_DOUBLE(REPLACE(Cell_W84Y, ',', '.')) AS longitude,
    UPPER(TRIM(mobile_manufacturer)) AS fabricant_mobile,
    UPPER(TRIM(mobile_type)) AS type_mobile,
  FROM {{ source('tel_insights_raw', 'bouygues_events') }}   -- Référence DBT à la table source RAW
  WHERE event_imsi IS NOT NULL
    AND event_code NOT IN ('', 'NULL')
)

SELECT * FROM base
