WITH source AS (
    -- On lit la table brute unifiée (qui contient Orange, SFR, etc.)
    SELECT * FROM {{ source('pnij_src', 'RAW_MT20') }}
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
        -- 2. GESTION ROBUSTE DES DATES (Le point critique)
        -- ==============================================================
        -- SFR livre souvent en UTC (ex: "01/05/2025 - 08:47:21 UTC")
        -- Orange livre souvent en Local (ex: "01/05/2025 - 10:47:21")
        -- Stratégie : Si on détecte "UTC", on nettoie et on convertit en heure de Paris.
        -- Sinon, on suppose que c'est déjà l'heure locale.
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
        -- 3. NETTOYAGE DES IDENTIFIANTS (Regex)
        -- ==============================================================
        -- Enlève les =("...") et les espaces pour ne garder que les chiffres
        REGEXP_REPLACE("CIBLE", '\\D', '')          AS msisdn_cible,
        REGEXP_REPLACE("CORRESPONDANT", '\\D', '')  AS msisdn_correspondant,

        -- Standardisation des textes
        UPPER(TRIM("TYPE"))                         AS type_evenement,
        UPPER(TRIM("DIRECTION"))                    AS sens_communication, -- 'SORTANT', 'ENTRANT'

        -- Durée : Conversion en entier (Secondes)
        TRY_CAST("DUREE" AS INT)                    AS duree_secondes,

        -- Identifiants Matériels
        REGEXP_REPLACE("IMSI", '\\D', '')           AS imsi,
        REGEXP_REPLACE("IMEI", '\\D', '')           AS imei,

        -- ==============================================================
        -- 4. LOCALISATION (Lambert vers GPS plus tard)
        -- ==============================================================
        -- On garde les noms explicites pour ne pas confondre avec du GPS
        TRY_CAST(REPLACE("X", ',', '.') AS FLOAT)   AS coord_x_lambert,
        TRY_CAST(REPLACE("Y", ',', '.') AS FLOAT)   AS coord_y_lambert,
        UPPER(TRIM("TYPE-COORD"))                   AS type_projection, -- ex: LAMBERT2ETENDU
        UPPER(TRIM("ADRESSE2"))                     AS adresse_antenne,
        UPPER(TRIM("ADRESSE3"))                     AS complement_adresse,
        UPPER(TRIM("CODE POSTAL"))                  AS cp_antenne,
        UPPER(TRIM("VILLE"))                        AS ville_antenne,
        UPPER(TRIM("PAYS"))                         AS pays_antenne

    FROM source
)

SELECT * FROM nettoyage
-- On filtre les lignes vides ou techniques qui auraient pu glisser
WHERE date_heure_utc_paris IS NOT NULL