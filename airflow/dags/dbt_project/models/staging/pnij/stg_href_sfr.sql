{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('pnij_src', 'RAW_HREF_SFR') }}
),

renamed AS (
    SELECT
        -- 1. MÉTADONNÉES TECHNIQUES
        DOSSIER_ID      AS dossier_id,
        SOURCE_FILENAME AS source_filename,
        LOADED_AT       AS loaded_at,

        -- Métadonnées du formulaire (Zone)
        UPPER(INPUT_ZONE_NAME) AS nom_zone_formulaire,

        -- CORRECTION ICI : Renommage pour standardisation
        INPUT_ZONE_NUM         AS numero_zone,

        -- 2. ÉVÉNEMENT & TEMPS
        TRY_TO_TIMESTAMP("Heure Eve. Reseau (heure de Paris)", 'DD/MM/YYYY HH24:MI:SS') AS date_heure_utc_paris,

        UPPER("Eve. Reseau") AS type_event,

        -- 3. IDENTIFIANTS
        TRIM("IMSI")   AS imsi,
        TRIM("IMEI")   AS imei,
        TRIM("MSISDN") AS msisdn,

        -- Infos Appareil
        UPPER("Marque Actuelle") AS constructeur_mobile,
        UPPER("Modèle Actuel")   AS modele_mobile,

        -- 4. LOCALISATION & ANTENNE
        TRIM("GCI") AS id_cellule_technique,

        UPPER("Adresse postale implantation du Site") AS adresse_antenne,
        "Code Postal implantation du Site"            AS cp_antenne,
        UPPER("Ville implantation du Site")           AS ville_antenne,

        -- DÉCOUPAGE LAT/LON (WGS84)
        TRY_TO_DOUBLE(SPLIT_PART("Coordonnées GPS du barycentre de la couverture de la cellule", ',', 1)) AS latitude,
        TRY_TO_DOUBLE(SPLIT_PART("Coordonnées GPS du barycentre de la couverture de la cellule", ',', 2)) AS longitude

    FROM source
)

SELECT * FROM renamed
-- Ajout d'un filtre de propreté standard
WHERE date_heure_utc_paris IS NOT NULL
AND imsi IS NOT NULL