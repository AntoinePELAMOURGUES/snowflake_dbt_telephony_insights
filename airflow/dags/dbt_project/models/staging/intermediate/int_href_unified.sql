{{ config(
    materialized='table',
    cluster_by=['dossier_id', 'numero_zone', 'date_heure_utc_fr']
) }}

WITH
-- =============================================================================
-- 1. PRÉPARATION DES SOURCES (MAPPING STRICT DES COLONNES)
-- =============================================================================

src_orange AS (
    SELECT
        dossier_id,
        source_filename,
        'ORANGE' AS operateur,
        numero_zone,
        nom_zone_formulaire,

        date_heure_utc_paris AS date_heure_utc_fr,

        imsi,
        imei,
        msisdn,

        -- Tech ,
        type_event AS description_event,

        -- Loc Brute
        adresse_antenne,
        cp_antenne,
        ville_antenne,
        x_lambert,
        y_lambert,
        CAST(NULL AS FLOAT) AS lat_native,
        CAST(NULL AS FLOAT) AS lon_native,

    FROM {{ ref('stg_href_orange') }}
),

src_sfr AS (
    SELECT
        dossier_id,
        source_filename,
        'SFR' AS operateur,
        numero_zone,
        nom_zone_formulaire,

        date_heure_utc_paris AS date_heure_utc_fr,

        imsi,
        imei,
        msisdn,

        -- Tech
        type_event AS description_event,

        -- Loc Brute
        adresse_antenne,
        cp_antenne,
        ville_antenne,
        CAST(NULL AS FLOAT) AS x_lambert,
        CAST(NULL AS FLOAT) AS y_lambert,
        latitude AS lat_native,
        longitude AS lon_native,

    FROM {{ ref('stg_href_sfr') }}
),

src_bouygues AS (
    SELECT
        dossier_id,
        source_filename,
        'BOUYGUES' AS operateur,
        numero_zone,
        nom_zone_formulaire,

        date_heure_utc_paris AS date_heure_utc_fr,

        imsi,
        imei,
        msisdn,

        -- Tech
        COALESCE(libelle_evenement, code_evenement) AS description_event,

        -- Loc Brute
        adresse_antenne,
        cp_antenne,
        ville_antenne,
        CAST(NULL AS FLOAT) AS x_lambert,
        CAST(NULL AS FLOAT) AS y_lambert,
        latitude as lat_native,
        longitude as lon_native,

    FROM {{ ref('stg_href_bouygues') }}
),

-- =============================================================================
-- 2. UNION DES DONNÉES
-- =============================================================================
unified AS (
    SELECT * FROM src_orange
    UNION ALL
    SELECT * FROM src_sfr
    UNION ALL
    SELECT * FROM src_bouygues
),

-- =============================================================================
-- 3. CALCUL GÉOLOCALISATION & ENRICHISSEMENT
-- =============================================================================
final_calc AS (
    SELECT
        u.*,

        -- Stratégie GPS : Si natif (SFR/Bouygues) on garde, sinon on convertit (Orange)
        CASE
            WHEN lat_native IS NOT NULL THEN lat_native
            WHEN x_lambert IS NOT NULL THEN
                UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(x_lambert, y_lambert):lat::FLOAT
            ELSE NULL
        END AS latitude,

        CASE
            WHEN lon_native IS NOT NULL THEN lon_native
            WHEN x_lambert IS NOT NULL THEN
                UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(x_lambert, y_lambert):lon::FLOAT
            ELSE NULL
        END AS longitude

    FROM unified u
)

SELECT
    dossier_id,
    source_filename,
    nom_zone_formulaire,
    numero_zone, -- CLÉ MAJEURE POUR LA CONFRONTATION
    operateur,
    date_heure_utc_fr,
    imsi,
    imei,
    msisdn,
    description_event,
    latitude,
    longitude,
    ville_antenne,
    TRIM(REGEXP_REPLACE(
            COALESCE(adresse_antenne, '') || ' ' ||
            COALESCE(cp_antenne, '') || ' ' ||
            COALESCE(ville_antenne, ''),
            '\\s+', ' ' -- Remplace "   " par " "
        )) AS adresse_cellule,

FROM final_calc
-- On filtre les lignes sans identifiant (bruit inutile)
WHERE imsi IS NOT NULL