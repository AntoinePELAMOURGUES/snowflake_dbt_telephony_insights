WITH
-- 1. On charge les MT20 FREE
src_mt20 AS (
    SELECT *, 'MT20' AS source_type
    FROM {{ source('pnij_src', 'RAW_MT20') }}
    WHERE source_filename LIKE '%FREE%'
),

-- 2. On charge les MT24 FREE
src_mt24 AS (
    SELECT *, 'MT24' AS source_type
    FROM {{ source('pnij_src', 'RAW_MT24') }}
    WHERE source_filename LIKE '%FREE%'
),
union_source AS (
    SELECT * FROM src_mt20
    UNION ALL
    SELECT * FROM src_mt24
),
-- 3. On les fusionne AVANT le nettoyage
-- C'est possible car les colonnes brutes sont strictement identiques
renamed AS (
    SELECT
        -- METADATA
        dossier_id,
        source_filename,
        source_type, -- Permet de savoir si ça vient du MT20 ou MT24
        'FREE' AS operateur,

        -- 1. DATES (Spécifique FREE)
        TRY_TO_TIMESTAMP(
            REGEXP_REPLACE("DATE", ' UTC.*', ''),
            'DD/MM/YYYY - HH24:MI:SS'
        ) AS date_heure_utc_fr,

        -- 2. IDENTIFIANTS TECHNIQUES
        -- Nettoyage des =("...") pour ne garder que les chiffres
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

        CASE
            WHEN "TYPE-COORD" = 'WGS84' THEN TRY_TO_DOUBLE("X")
            ELSE NULL
        END AS latitude, -- Pas de conversion Lambert nécessaire

        CASE
            WHEN "TYPE-COORD" = 'WGS84' THEN TRY_TO_DOUBLE("Y")
            ELSE NULL
        END AS longitude, -- Pas de conversion Lambert nécessaire

        CAST(NULL AS DOUBLE) AS x_lambert,
        CAST(NULL AS DOUBLE) AS y_lambert,

    FROM union_source
)

SELECT * FROM renamed
WHERE date_heure_utc_fr IS NOT NULL -- On écarte les lignes vides ou en-têtes répétés