{{ config(
    materialized='table',
    cluster_by=['dossier_id', 'date_heure_utc_fr']
) }}

WITH
-- =============================================================================
-- 1. RASSEMBLEMENT AVEC SÉLECTION EXPLICITE
-- On cite les colonnes pour garantir qu'elles s'alignent parfaitement
-- =============================================================================
unified_source AS (
    -- ORANGE
    SELECT
        dossier_id, source_filename, source_type, operateur,
        date_heure_utc_fr,
        msisdn, imsi, imei,
        type_communication, direction, duree_secondes, msisdn_correspondant,
        adresse, adresse_complement, code_postal, ville,
        x_lambert, y_lambert, latitude, longitude
    FROM {{ ref('stg_mt_orange') }}

    UNION ALL

    -- SFR
    SELECT
        dossier_id, source_filename, source_type, operateur,
        date_heure_utc_fr,
        msisdn, imsi, imei,
        type_communication, direction, duree_secondes, msisdn_correspondant,
        adresse, adresse_complement, code_postal, ville,
        x_lambert, y_lambert, latitude, longitude
    FROM {{ ref('stg_mt_sfr') }}

    UNION ALL

    -- BOUYGUES
    SELECT
        dossier_id, source_filename, source_type, operateur,
        date_heure_utc_fr,
        msisdn, imsi, imei,
        type_communication, direction, duree_secondes, msisdn_correspondant,
        adresse, adresse_complement, code_postal, ville,
        x_lambert, y_lambert, latitude, longitude
    FROM {{ ref('stg_mt_bouygues') }}

    UNION ALL

    -- FREE
    SELECT
        dossier_id, source_filename, source_type, operateur,
        date_heure_utc_fr,
        msisdn, imsi, imei,
        type_communication, direction, duree_secondes, msisdn_correspondant,
        adresse, adresse_complement, code_postal, ville,
        x_lambert, y_lambert, latitude, longitude
    FROM {{ ref('stg_mt_free') }}
),

-- =============================================================================
-- 2. ENRICHISSEMENT & CALCUL GÉOGRAPHIQUE
-- =============================================================================
geo_calc AS (
    SELECT
        -- MÉTADONNÉES
        dossier_id,
        source_filename,
        source_type,
        operateur,

        -- TEMPS
        date_heure_utc_fr,

        -- IDENTIFIANTS
        msisdn,
        imsi,
        imei,

        -- DÉTAILS
        type_communication,
        direction,
        duree_secondes,
        msisdn_correspondant,

        -- ADRESSES & GÉO (Brutes)
        adresse,
        adresse_complement, -- Je garde le nom original ici, on renomme à la fin si besoin
        code_postal,
        ville,
        x_lambert,
        y_lambert,

        -- CALCUL GÉOLOC (Priorité GPS > Conversion Lambert)
        CASE
            WHEN latitude IS NOT NULL THEN latitude
            WHEN x_lambert IS NOT NULL THEN
                UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(x_lambert, y_lambert):lat::FLOAT
            ELSE NULL
        END AS latitude_finale,

        CASE
            WHEN longitude IS NOT NULL THEN longitude
            WHEN x_lambert IS NOT NULL THEN
                UTILS_DB.PUBLIC.UDF_LAMBERT2_TO_GPS(x_lambert, y_lambert):lon::FLOAT
            ELSE NULL
        END AS longitude_finale

    FROM unified_source
)

-- =============================================================================
-- 3. SÉLECTION FINALE & DÉDOUBLONNAGE
-- =============================================================================
SELECT
    dossier_id,
    source_filename,
    source_type,
    operateur,
    date_heure_utc_fr,
    msisdn,
    imsi,
    imei,
    type_communication,
    direction,
    duree_secondes,
    msisdn_correspondant,

    adresse,
    adresse_complement AS complement_adresse, -- Renommage demandé dans ton snippet
    code_postal,
    ville,

    -- On garde les coords techniques pour vérification
    x_lambert,
    y_lambert,

    -- Coordonnées finales unifiées
    latitude_finale AS latitude,
    longitude_finale AS longitude

FROM geo_calc
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY msisdn, date_heure_utc_fr, duree_secondes, type_communication, dossier_id
    ORDER BY source_filename DESC
) = 1