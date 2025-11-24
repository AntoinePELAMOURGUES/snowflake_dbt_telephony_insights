{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('pnij_src', 'RAW_ANNUAIRE') }}
),

renamed AS (
    SELECT
        -- 1. MÉTADONNÉES TECHNIQUES (Traçabilité)
        DOSSIER_ID      AS dossier_id,
        SOURCE_FILENAME AS source_filename,
        LOADED_AT       AS loaded_at,

        -- 2. IDENTIFIANTS CLÉS (Pour les jointures avec MT20/MT24)
        -- On nettoie les espaces potentiels qui cassent souvent les jointures
        TRIM("_ficheNumero")    AS msisdn,
        TRIM("_ficheIMSI")      AS imsi,
        TRIM("_ficheSIM")       AS iccid,
        TRIM("_ficheIMEIvendu") AS imei_vendu, -- Utile pour lier un téléphone vendu avec la ligne

        -- 3. IDENTITÉ TITULAIRE
        -- Logique : Si "Nom" est vide, on tente de prendre la "Raison Sociale" ou le "Représentant Légal"
        UPPER(COALESCE(NULLIF("_personneNom", ''), "_representantLegalNom", "_personneRaisonSociale")) AS nom,
        UPPER(COALESCE(NULLIF("_personnePrenom", ''), "_representantLegalPrenom"))                     AS prenom,

        UPPER("_personneRaisonSociale") AS raison_sociale,
        UPPER("_personneSurnom")        AS alias,

        -- 4. ADRESSE & CONTACT
        UPPER(COALESCE("_personneAdresse", "_representantLegalAdresse")) AS adresse_voie,
        COALESCE("_personneCodePostal", "_representantLegalCodePostal")  AS code_postal,
        UPPER(COALESCE("_personneVille", "_representantLegalVille"))     AS ville,
        UPPER("_personnePays")                                           AS pays,

        -- 5. CONTRAT & OFFRE
        UPPER("_ficheOperateur")   AS operateur,
        UPPER("_ficheTypeContrat") AS type_contrat, -- Postpayé / Prépayé

        -- Tentative de conversion de date (Format PNIJ souvent instable, on utilise TRY_TO_DATE)
        TRY_TO_DATE("_ficheDebutAbonnement", 'DD/MM/YYYY') AS date_debut_abonnement,
        TRY_TO_DATE("_ficheFinAbonnement", 'DD/MM/YYYY')   AS date_fin_abonnement

    FROM source
)

SELECT * FROM renamed