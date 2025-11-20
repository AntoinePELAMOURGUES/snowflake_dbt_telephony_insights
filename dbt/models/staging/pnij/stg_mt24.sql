WITH source AS (
    SELECT * FROM {{ source('pnij_src', 'RAW_MT24') }}
),

nettoyage AS (
    SELECT
        -- ==============================================================
        -- 1. MÉTADONNÉES TECHNIQUES & DOSSIER
        -- ==============================================================
        DOSSIER_ID              AS id_dossier,
        SOURCE_FILENAME         AS nom_fichier_source,
        LOADED_AT               AS date_chargement,


        -- ==============================================================
        -- 2. GESTION DES DATES (Même logique robuste que MT20)
        -- ==============================================================
        CASE
            WHEN "DATE" LIKE '%UTC%' THEN
                CONVERT_TIMEZONE(
                    'UTC',
                    'Europe/Paris',
                    TRY_TO_TIMESTAMP(REPLACE("DATE", ' UTC', ''), 'DD/MM/YYYY - HH24:MI:SS')
                )
            ELSE
                TRY_TO_TIMESTAMP("DATE", 'DD/MM/YYYY - HH24:MI:SS')
        END AS date_heure_utc_paris,

        -- ==============================================================
        -- 3. IDENTIFIANTS (Focus sur l'IMEI)
        -- ==============================================================
        -- L'IMEI est la clé de voûte du MT24
        REGEXP_REPLACE("IMEI", '\\D', '')           AS imei_appareil,

        -- L'IMSI est la carte SIM présente dans l'appareil
        REGEXP_REPLACE("IMSI", '\\D', '')           AS imsi_carte_sim,

        -- La colonne "CIBLE" dans le CSV contient souvent le MSISDN (Numéro de tel)
        -- On le renomme "msisdn_associe" car c'est le numéro inséré dans le téléphone à ce moment-là
        REGEXP_REPLACE("CIBLE", '\\D', '')          AS msisdn_associe,

        REGEXP_REPLACE("CORRESPONDANT", '\\D', '')  AS msisdn_correspondant,

        -- ==============================================================
        -- 4. DÉTAILS ÉVÉNEMENT
        -- ==============================================================
        UPPER(TRIM("TYPE"))                         AS type_evenement,
        UPPER(TRIM("DIRECTION"))                    AS sens_communication,
        TRY_CAST("DUREE" AS INT)                    AS duree_secondes,

        -- ==============================================================
        -- 5. LOCALISATION
        -- ==============================================================
        TRY_CAST(REPLACE("X", ',', '.') AS FLOAT)   AS coord_x_lambert,
        TRY_CAST(REPLACE("Y", ',', '.') AS FLOAT)   AS coord_y_lambert,
        UPPER(TRIM("TYPE-COORD"))                   AS type_projection,
        UPPER(TRIM("ADRESSE2"))                     AS adresse_antenne,
        UPPER(TRIM("ADRESSE3"))                     AS complement_adresse,
        UPPER(TRIM("CODE POSTAL"))                  AS cp_antenne,
        UPPER(TRIM("VILLE"))                        AS ville_antenne,
        UPPER(TRIM("PAYS"))                         AS pays_antenne

    FROM source
)

SELECT * FROM nettoyage
WHERE date_heure_utc_paris IS NOT NULL