{{ config(
    materialized='view'
) }}

WITH events AS (
    -- 1. Préparation des Événements (Communications)
    SELECT
        DOSSIER_ID,
        SOURCE_FILENAME,
        LOADED_AT,
        UPPER(INPUT_ZONE_NAME) AS INPUT_ZONE_NAME, -- Standardisation Nom Zone
        INPUT_ZONE_NUM,

        -- Nettoyage des identifiants
        TRIM("IMSI")   AS imsi,
        TRIM("IMEI")   AS imei,
        TRIM("MSISDN") AS msisdn,

        -- Gestion de l'horodatage
        TRY_TO_TIMESTAMP("Horodatage_debut", 'YYYY-MM-DD HH24:MI:SS') AS date_heure_utc_paris,

        -- Clé de jointure (Nettoyage espaces)
        REPLACE(TRIM("Cellule"), ' ', '') AS cell_id_key,

        UPPER("Technologie") AS technologie, -- ex: 4G, 5G
        UPPER("TypeCDR")     AS type_event,  -- ex: VOIX, SMS
        UPPER("SousTypeCDR") AS sous_type_event

    FROM {{ source('pnij_src', 'RAW_HREF_EVENTS_ORANGE') }}
),

cells AS (
    -- 2. Préparation du Référentiel Cellules (Positions)
    SELECT
        DOSSIER_ID,
        INPUT_ZONE_NUM,

        -- Clé de jointure
        COALESCE(
            NULLIF(TRIM("CellID2"), ''),
            REPLACE(TRIM("CellID"), ' ', '')
        ) AS cell_id_key,

        UPPER("Nom")     AS nom_antenne,
        UPPER("Adresse") AS adresse_antenne,
        "CP"             AS cp_antenne,
        UPPER("Ville")   AS ville_antenne,
        "Azimut"         AS azimut,

        -- Nettoyage Coordonnées (Virgule -> Point)
        TRY_TO_DOUBLE(REPLACE("X Lambert", ',', '.')) AS x_lambert,
        TRY_TO_DOUBLE(REPLACE("Y Lambert", ',', '.')) AS y_lambert

    FROM {{ source('pnij_src', 'RAW_HREF_CELLS_ORANGE') }}
),

joined AS (
    -- 3. Jointure et Consolidation
    SELECT
        e.dossier_id,
        e.source_filename,
        e.loaded_at,
        e.INPUT_ZONE_NAME   AS nom_zone_formulaire,

        e.INPUT_ZONE_NUM as numero_zone,

        e.date_heure_utc_paris,
        e.imsi,
        e.imei,
        e.msisdn,

        e.technologie,
        e.type_event,
        e.sous_type_event,

        -- Données enrichies (déjà en UPPER via la CTE cells)
        c.nom_antenne,
        c.adresse_antenne,
        c.cp_antenne,
        c.ville_antenne,
        c.x_lambert,
        c.y_lambert,
        c.azimut,

        e.cell_id_key       AS id_cellule_technique

    FROM events e
    LEFT JOIN cells c
        ON e.DOSSIER_ID = c.DOSSIER_ID
        AND e.INPUT_ZONE_NUM = c.INPUT_ZONE_NUM
        AND e.cell_id_key = c.cell_id_key
)

SELECT * FROM joined