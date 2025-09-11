-- Role utilisé : RAW_DATA_READER (lecture S3, écriture dans RAW)
-- Warehouse : tel_insights_etl
-- Database : tel_insights_raw
-- Schema : event_raw
-- Tables recommandées par zone :

-- On se place sur le rôle sysadmin pour la création d'objets globaux
use role sysadmin;

-- Activation du warehouse dédié aux opérations ETL (import et nettoyage)
use warehouse tel_insights_etl;

-- Sélection du schéma cible pour l'import des données brutes Bouygues
use schema tel_insights_raw.event_raw;

-- Création de la table pour la zone 1
-- Structure adaptée à la nomenclature des fichiers Bouygues (correspondance avec les colonnes du CSV)
CREATE OR REPLACE TABLE bouygues_zone_1 (
    Event_StartTime TIMESTAMP,             -- Date et heure de l'événement (au bon format pour analyse temporelle)
    Event_IMSI VARCHAR,                    -- Identifiant IMSI du mobile
    Event_MSISDN VARCHAR,                  -- Numéro MSISDN (ligne)
    Event_IMEI VARCHAR,                    -- IMEI du téléphone
    Adresse1_Numero VARCHAR,               -- Numéro de voie/adresse
    Adresse2_VoieLieu VARCHAR,             -- Voie/Lieu dit
    Adresse3_CodePostal VARCHAR,           -- Code postal
    Adresse4_Ville VARCHAR,                -- Ville
    Event_Code VARCHAR,                    -- Code événement technique
    Event_Libl_Fr VARCHAR,                 -- Libellé événement (français)
    Event_Country VARCHAR,                 -- Pays du réseau
    Network VARCHAR,                       -- Nom du réseau opérateur
    Cell_Techno VARCHAR,                   -- Technologie cellulaire (3G/4G/5G)
    Cell_Name VARCHAR,                     -- Nom de la cellule
    Cell_eCGI VARCHAR,                     -- Identifiant eCGI de la cellule
    Cell_W84X VARCHAR,                     -- Coordonnée X WGS84
    Cell_W84Y VARCHAR,                     -- Coordonnée Y WGS84
    Event_Libl_En VARCHAR,                 -- Libellé événement (anglais)
    Event_Type VARCHAR,                    -- Type d'événement
    Heure_de_Event_StartTime VARCHAR,      -- Heure complémentaire de l'événement (si présente)
    Mobile_Manufacturer VARCHAR,           -- Constructeur du téléphone
    Mobile_Type VARCHAR,                   -- Modèle du téléphone
    Max_Cell_Azimut INTEGER                -- Azimut maximal de la cellule
);

-- Création de la table pour la zone 2 (structure identique à zone 1)
CREATE OR REPLACE TABLE bouygues_zone_2 (
    Event_StartTime TIMESTAMP,
    Event_IMSI VARCHAR,
    Event_MSISDN VARCHAR,
    Event_IMEI VARCHAR,
    Adresse1_Numero VARCHAR,
    Adresse2_VoieLieu VARCHAR,
    Adresse3_CodePostal VARCHAR,
    Adresse4_Ville VARCHAR,
    Event_Code VARCHAR,
    Event_Libl_Fr VARCHAR,
    Event_Country VARCHAR,
    Network VARCHAR,
    Cell_Techno VARCHAR,
    Cell_Name VARCHAR,
    Cell_eCGI VARCHAR,
    Cell_W84X VARCHAR,
    Cell_W84Y VARCHAR,
    Event_Libl_En VARCHAR,
    Event_Type VARCHAR,
    Heure_de_Event_StartTime VARCHAR,
    Mobile_Manufacturer VARCHAR,
    Mobile_Type VARCHAR,
    Max_Cell_Azimut INTEGER
);

-- Création de la table pour la zone 3 (structure identique à zone 1 & 2)
CREATE OR REPLACE TABLE bouygues_zone_3 (
    Event_StartTime TIMESTAMP,
    Event_IMSI VARCHAR,
    Event_MSISDN VARCHAR,
    Event_IMEI VARCHAR,
    Adresse1_Numero VARCHAR,
    Adresse2_VoieLieu VARCHAR,
    Adresse3_CodePostal VARCHAR,
    Adresse4_Ville VARCHAR,
    Event_Code VARCHAR,
    Event_Libl_Fr VARCHAR,
    Event_Country VARCHAR,
    Network VARCHAR,
    Cell_Techno VARCHAR,
    Cell_Name VARCHAR,
    Cell_eCGI VARCHAR,
    Cell_W84X VARCHAR,
    Cell_W84Y VARCHAR,
    Event_Libl_En VARCHAR,
    Event_Type VARCHAR,
    Heure_de_Event_StartTime VARCHAR,
    Mobile_Manufacturer VARCHAR,
    Mobile_Type VARCHAR,
    Max_Cell_Azimut INTEGER
);


-- Import des données de la zone 1 avec COPY INTO depuis le stage S3
COPY INTO tel_insights_raw.event_raw.bouygues_zone_1
FROM '@my_s3_stage/Evenements Zone 1_data.csv'
FILE_FORMAT = (FORMAT_NAME = 'mon_format_csv')
ON_ERROR = 'CONTINUE';    -- Ignore les lignes erronées pour garantir le chargement global

-- Import des données de la zone 2 (de la même manière, dans la table dédiée)
COPY INTO tel_insights_raw.event_raw.bouygues_zone_2
FROM '@my_s3_stage/Evenements Zone 2_data.csv'
FILE_FORMAT = (FORMAT_NAME = 'mon_format_csv')
ON_ERROR = 'CONTINUE';

-- Import des données de la zone 3 (idem)
COPY INTO tel_insights_raw.event_raw.bouygues_zone_3
FROM '@my_s3_stage/Evenements Zone 3_data.csv'
FILE_FORMAT = (FORMAT_NAME = 'mon_format_csv')
ON_ERROR = 'CONTINUE';
