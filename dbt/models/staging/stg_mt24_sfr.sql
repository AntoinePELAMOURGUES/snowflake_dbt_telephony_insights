-- models/staging/stg_mt24_sfr_csv.sql

WITH source AS (

    -- Référence à la table RAW via la macro 'source' de dbt
    SELECT * FROM {{ source('raw', 'src_mt24_sfr_csv') }}

),

renamed_typed_parsed AS (

    SELECT
        -- Métadonnées de la communication (Fix 1: Majuscule)
        UPPER("TYPE") AS type_evenement,

        -- Conversion de la date en timestamp
        -- Note: Le format 'DD/MM/YYYY - HH24:MI:SS' est spécifique à cet opérateur
        TRY_TO_TIMESTAMP("DATE", 'DD/MM/YYYY - HH24:MI:SS') AS date_heure_evenement,

        -- Nettoyage des identifiants (Fix 2, 3: REGEXP_REPLACE)
        -- On supprime tous les caractères non numériques ( \D ) pour isoler l'identifiant
        REGEXP_REPLACE("CIBLE", '\\D', '') AS msisdn_cible,
        REGEXP_REPLACE("CORRESPONDANT", '\\D', '') AS msisdn_correspondant,

        -- (Fix 4: Majuscule)
        UPPER("DIRECTION") AS direction,

        -- Conversion de la durée en entier (gestion des erreurs avec TRY_CAST)
        TRY_CAST("DUREE" AS INT) AS duree_secondes,

        -- Identifiants techniques (Fix 5: REGEXP_REPLACE)
        REGEXP_REPLACE("IMSI", '\\D', '') AS imsi,
        REGEXP_REPLACE("IMEI", '\\D', '') AS imei,

        -- Données de localisation (Antenne) (Fix 6: Majuscules)
        UPPER("ADRESSE2") AS adresse_antenne,
        UPPER("ADRESSE3") AS comp_adresse_antenne,
        "CODE POSTAL" AS code_postal_antenne, -- Pas de majuscule demandée pour le CP
        UPPER("VILLE") AS ville_antenne,
        UPPER("PAYS") AS pays_antenne,
        UPPER("TYPE-COORD") AS type_coordonnees,

        -- Coordonnées
        "X" AS coord_x,
        "Y" AS coord_y

    FROM source

)

SELECT * FROM renamed_typed_parsed