WITH source AS (
    SELECT * FROM {{ source('pnij_src', 'RAW_HREF_BOUYGUES') }}
),

nettoyage AS (
    SELECT
        -- ==============================================================
        -- 1. MÉTADONNÉES DOSSIER & ZONE (Crucial pour la confrontation)
        -- ==============================================================
        DOSSIER_ID              AS id_dossier,
        SOURCE_FILENAME         AS nom_fichier_source,

        -- L'identifiant de zone saisi dans Streamlit (ex: "1", "2")
        -- C'est la clé de jointure pour confronter avec Orange/SFR
        INPUT_ZONE_NUM          AS numero_zone,
        -- ==============================================================
        -- 2. TEMPS & ÉVÉNEMENT
        -- ==============================================================
        -- Format Bouygues habituel : DD/MM/YYYY HH:MI:SS
        TRY_TO_TIMESTAMP("Event.StartTime", 'DD/MM/YYYY HH24:MI:SS') AS date_heure_utc_paris,

        UPPER(TRIM("Event.Code"))       AS code_evenement,
        upper(TRIM("Event.Libl_Fr"))           AS libelle_evenement,

        -- ==============================================================
        -- 3. IDENTIFIANTS (Nettoyage standard)
        -- ==============================================================
        REGEXP_REPLACE("Event.IMSI", '\\D', '')     AS imsi,
        REGEXP_REPLACE("Event.IMEI", '\\D', '')     AS imei,
        REGEXP_REPLACE("Event.MSISDN", '\\D', '')   AS msisdn,

        -- ==============================================================
        -- 4. LOCALISATION & ANTENNE
        -- ==============================================================
        -- Correction Virgule -> Point pour les coordonnées
        -- Analyse des données : X ~45 (Lat), Y ~5 (Lon) pour la France
        TRY_CAST(REPLACE("Cell.W84X", ',', '.') AS FLOAT) AS latitude,
        TRY_CAST(REPLACE("Cell.W84Y", ',', '.') AS FLOAT) AS longitude,

        TRIM("Cell.Techno")             AS technologie, -- 4G, 5G...

        UPPER(TRIM("Adresse4.Ville"))   AS ville_antenne,
        TRIM("Adresse3.CodePostal")     AS cp_antenne,

        -- Reconstitution d'une adresse complète
        UPPER(TRIM(
            COALESCE("Adresse1.Numero", '') || ' ' ||
            COALESCE("Adresse2.Voie-Lieu", '')
        )) AS adresse_antenne,
        UPPER(TRIM("Mobile.Type")) AS modele_IMEI -- Smartphone, Tablette...

    FROM source
)

SELECT * FROM nettoyage
WHERE date_heure_utc_paris IS NOT NULL
AND imsi IS NOT NULL -- On ne garde que ce qui est identifiable