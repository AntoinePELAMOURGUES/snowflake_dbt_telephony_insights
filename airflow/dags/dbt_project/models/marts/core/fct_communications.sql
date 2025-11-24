{{ config(
    materialized='table',
    cluster_by=['dossier_id', 'date_heure_utc_fr']
) }}

WITH
-- 1. On charge les communications unifiées
coms AS (
    SELECT * FROM {{ ref('int_communications_all') }}
),

-- 2. On charge l'annuaire
annuaire AS (
    SELECT * FROM {{ ref('stg_annuaire') }}
),

-- 3. ENRICHISSEMENT & CONCATÉNATION
joined AS (
    SELECT
        -- CLÉS TECHNIQUES
        c.dossier_id,
        c.source_filename,
        c.operateur,
        c.source_type, -- MT20 ou MT24

        -- TEMPS
        c.date_heure_utc_fr,

        -- CIBLE (La ligne ou le boîtier analysé)
        c.msisdn AS msisdn_cible,
        -- IDENTIFICATION CIBLE (Qui est-ce ?)
        -- Concaténation Nom + Prénom ou Raison Sociale
        COALESCE(
            NULLIF(TRIM(a_cible.nom || ' ' || COALESCE(a_cible.prenom, '')), ''),
            'INCONNU'
        ) AS nom_cible,
        c.imei   AS imei_cible,
        c.imsi   AS imsi_cible,
        -- DÉTAILS COMMUNICATION
        c.type_communication, -- VOIX, SMS...
        c.direction,          -- ENTRANT, SORTANT
        c.duree_secondes,

        -- CORRESPONDANT (L'autre partie)
        c.msisdn_correspondant,

        -- IDENTIFICATION CORRESPONDANT (Qui est l'autre ?)
        COALESCE(
            NULLIF(TRIM(a_corr.nom || ' ' || COALESCE(a_corr.prenom, '')), ''),
            'INCONNU'
        ) AS nom_correspondant,

        -- =====================================================================
        -- ADRESSE CONCATÉNÉE & PROPRE
        -- =====================================================================
        -- On colle tout avec des espaces, puis on utilise REGEXP_REPLACE
        -- pour supprimer les espaces multiples si un champ (ex: complement) est vide.
        TRIM(REGEXP_REPLACE(
            COALESCE(c.adresse, '') || ' ' ||
            COALESCE(c.complement_adresse, '') || ' ' ||
            COALESCE(c.code_postal, '') || ' ' ||
            COALESCE(c.ville, ''),
            '\\s+', ' ' -- Remplace "   " par " "
        )) AS adresse_cellule,

        -- On garde quand même la ville pour les filtres rapides dans Streamlit
        c.ville as ville_cellule,

        -- GÉOLOCALISATION FINALE (Lat/Lon unifiées)
        c.latitude,
        c.longitude,

        -- INFOS TECHNIQUES (Pour audit si besoin)
        c.x_lambert,
        c.y_lambert

    FROM coms c

    -- Jointure 1 : Identifier la Cible (Le propriétaire de la ligne analysée)
    LEFT JOIN annuaire a_cible
        ON c.dossier_id = a_cible.dossier_id
        AND c.msisdn = a_cible.msisdn

    -- Jointure 2 : Identifier le Correspondant (Avec qui elle parle ?)
    LEFT JOIN annuaire a_corr
        ON c.dossier_id = a_corr.dossier_id
        AND c.msisdn_correspondant = a_corr.msisdn
)

SELECT * FROM joined