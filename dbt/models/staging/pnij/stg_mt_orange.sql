WITH
-- 1. On charge les MT20 ORANGE
src_mt20 AS (
    SELECT *, 'MT20' AS source_type
    FROM {{ source('pnij_src', 'RAW_MT20') }}
    WHERE source_filename LIKE '%ORANGE%'
),

-- 2. On charge les MT24 ORANGE
src_mt24 AS (
    SELECT *, 'MT24' AS source_type
    FROM {{ source('pnij_src', 'RAW_MT24') }}
    WHERE source_filename LIKE '%ORANGE%'
),

-- 3. On les fusionne AVANT le nettoyage
-- C'est possible car les colonnes brutes sont strictement identiques
union_source AS (
    SELECT * FROM src_mt20
    UNION ALL
    SELECT * FROM src_mt24
),

renamed AS (
    SELECT
        -- METADATA
        dossier_id,
        source_filename,
        source_type, -- Permet de savoir si ça vient du MT20 ou MT24
        'ORANGE' AS operateur,

        -- 2. DATES
        -- Format Orange standard : "01/07/2025 - 03:47:05"
        TRY_TO_TIMESTAMP("DATE", 'DD/MM/YYYY - HH24:MI:SS') AS date_heure_utc_fr,

        -- 1. IDENTIFIANTS
        REGEXP_REPLACE("CIBLE", '[^0-9]', '') AS msisdn,
        REGEXP_REPLACE("IMSI", '[^0-9]', '') AS imsi,
        REGEXP_REPLACE("IMEI", '[^0-9]', '') AS imei,

        "TYPE" AS type_communication,
        UPPER("DIRECTION") AS direction,
        TRY_TO_NUMBER("DUREE") AS duree_secondes,

        -- DETAILS
        REGEXP_REPLACE("CORRESPONDANT", '[^0-9]', '') AS msisdn_correspondant,

        -- GÉOLOCALISATION (Spécifique Bouygues : WGS84)
        "CELLID" AS cell_id,
        UPPER("ADRESSE2") AS adresse,
        UPPER("ADRESSE3") AS adresse_complement,
        "CODE POSTAL" AS code_postal,
        UPPER("VILLE") AS ville,

        -- On garde les valeurs brutes Lambert pour conversion future
        TRY_TO_DOUBLE("X") AS x_lambert,
        TRY_TO_DOUBLE("Y") AS y_lambert,

        -- On force NULL pour Lat/Lon car ce n'est pas du GPS
        CAST(NULL AS DOUBLE) AS latitude,
        CAST(NULL AS DOUBLE) AS longitude,


    FROM union_source
)

SELECT * FROM renamed
WHERE date_heure_utc_fr IS NOT NULL