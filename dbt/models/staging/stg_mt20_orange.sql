-- models/staging/stg_mt20_orange_csv.sql

WITH source AS (

    -- Référence à la table RAW via la macro 'source' de dbt
    SELECT * FROM {{ source('raw', 'src_mt20_orange_csv') }}

),

renamed_typed_parsed AS (

    SELECT
        -- Métadonnées de la communication
        "TYPE" AS type_evenement,

        -- Conversion de la date en timestamp
        -- Note: Le format 'DD/MM/YYYY - HH24:MI:SS' est spécifique à cet opérateur
        TRY_TO_TIMESTAMP("DATE", 'DD/MM/YYYY - HH24:MI:SS') AS date_heure_evenement,

        -- Nettoyage des identifiants (formatage PNIJ ="XXXXXXXX")
        REPLACE(REPLACE("CIBLE", '="', ''), '"', '') AS msisdn_cible,
        REPLACE(REPLACE("CORRESPONDANT", '="', ''), '"', '') AS msisdn_correspondant,

        "DIRECTION" AS direction,

        -- Conversion de la durée en entier (gestion des erreurs avec TRY_CAST)
        TRY_CAST("DUREE" AS INT) AS duree_secondes,

        -- Identifiants techniques
        REPLACE(REPLACE("IMSI", '="', ''), '"', '') AS imsi,
        REPLACE(REPLACE("IMEI", '="', ''), '"', '') AS imei,

        -- Données de localisation (Antenne)
        "ADRESSE2" AS adresse_antenne,
        "ADRESSE3" AS comp_adresse_antenne,
        "CODE POSTAL" AS code_postal_antenne,
        "VILLE" AS ville_antenne,
        "PAYS" AS pays_antenne,
        "TYPE-COORD" AS type_coordonnees,
        "X" AS coord_x,
        "Y" AS coord_y

    FROM source

)

SELECT * FROM renamed_typed_parsed