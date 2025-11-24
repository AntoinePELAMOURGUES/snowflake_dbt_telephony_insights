{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_annuaire') }}
),

enriched AS (
    SELECT
        -- 1. ON GARDE LES CLÉS ET METADATA
        dossier_id,
        source_filename,
        msisdn,

        -- 2. IDENTITÉ CONCATÉNÉE (PROPRE)
        -- Si prenom est null, on affiche juste le nom. On supprime les espaces multiples.
        TRIM(REGEXP_REPLACE(
            COALESCE(nom, '') || ' ' || COALESCE(prenom, ''),
            '\\s+', ' '
        )) AS identite_complete,

        -- 3. ADRESSE CONCATÉNÉE (POUR RECHERCHE GPS)
        -- On construit une chaîne "Voie CP Ville Pays" optimisée pour les moteurs de géocodage
        TRIM(REGEXP_REPLACE(
            COALESCE(adresse_voie, '') || ' ' ||
            COALESCE(code_postal, '') || ' ' ||
            COALESCE(ville, '') || ' ' ||
            COALESCE(pays, ''),
            '\\s+', ' '
        )) AS adresse_complete_recherche,

        -- On garde les champs séparés pour l'affichage ou le filtrage
        adresse_voie,
        code_postal,
        ville,

        -- 4. OFFRE
        operateur,
        date_debut_abonnement,
        date_fin_abonnement

    FROM source
)

SELECT * FROM enriched