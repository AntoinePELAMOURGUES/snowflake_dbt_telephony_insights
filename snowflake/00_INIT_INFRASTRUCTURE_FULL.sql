-- ===========================================================================
-- 1. GESTION DES RÔLES ET UTILISATEURS (SÉCURITÉ)
-- ===========================================================================

-- --- A. Création des Rôles Applicatifs ---
-- Séparation stricte : Le Frontend (Streamlit) ne doit pas avoir les droits du Backend (dbt)
CREATE ROLE IF NOT EXISTS STREAMLIT_ROLE COMMENT = 'Rôle de service pour l''application Streamlit (Lecture Marts / Ecriture Raw)';
CREATE ROLE IF NOT EXISTS DBT_ROLE       COMMENT = 'Rôle de transformation pour dbt (Full Access Staging/Marts)';
CREATE ROLE IF NOT EXISTS AIRFLOW_ROLE   COMMENT = 'Rôle d''orchestration pour Airflow (Déclencheur)';

-- --- B. Création des Utilisateurs de Service ---

-- 1. Utilisateur STREAMLIT (Authentification par Clé RSA)
-- Note : La clé publique ci-dessous est un exemple. En prod, générez la vôtre avec :
-- $ openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
-- $ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
CREATE USER IF NOT EXISTS STREAMLIT_APP_USER
    RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4Pdih1fHCPhIvw6BAj9d...' -- Remplacez par votre VRAIE clé publique
    DEFAULT_ROLE = STREAMLIT_ROLE
    DEFAULT_WAREHOUSE = STREAMLIT_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Compte de service Streamlit sécurisé par paire de clés';

-- 2. Utilisateur DBT (Authentification par Mot de passe pour simplifier le dev, ou Key-Pair si possible)
CREATE USER IF NOT EXISTS DBT_USER
    PASSWORD = 'VOTRE_MOT_DE_PASSE_FORT_ICI' -- À gérer via un gestionnaire de secrets
    LOGIN_NAME = 'DBT_USER'
    DEFAULT_ROLE = DBT_ROLE
    DEFAULT_WAREHOUSE = DBT_WH
    COMMENT = 'Compte de service pour les transformations dbt';

-- --- C. Hiérarchie des Rôles ---
-- Lier les utilisateurs aux rôles
GRANT ROLE STREAMLIT_ROLE TO USER STREAMLIT_APP_USER;
GRANT ROLE DBT_ROLE TO USER DBT_USER;

-- Airflow doit pouvoir agir comme dbt pour lancer les jobs
GRANT ROLE DBT_ROLE TO ROLE AIRFLOW_ROLE;

-- SYSADMIN doit avoir la main sur tout pour la maintenance
GRANT ROLE STREAMLIT_ROLE TO ROLE SYSADMIN;
GRANT ROLE DBT_ROLE TO ROLE SYSADMIN;

-- ===========================================================================
-- 2. GESTION DU COMPUTE (WAREHOUSES)
-- ===========================================================================

-- Repasser en SYSADMIN pour la création d'objets (Bonne pratique)
USE ROLE SYSADMIN;

-- Warehouse "Intéractif" (Streamlit)
-- Optimisé pour beaucoup de petites requêtes rapides
CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60          -- Arrêt rapide pour économiser les coûts
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute pour l''UI Streamlit et l''ingestion de petits fichiers';

-- Warehouse "Batch" (Transformation)
-- Optimisé pour le traitement de données (Jointures, Agrégations)
CREATE WAREHOUSE IF NOT EXISTS DBT_WH
    WAREHOUSE_SIZE = 'SMALL'   -- Taille SMALL recommandée pour dbt si volumes > 1Go
    AUTO_SUSPEND = 300         -- 5 min de délai car dbt enchaîne les requêtes
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pour les transformations dbt et Airflow';

-- ===========================================================================
-- 3. ARCHITECTURE DES DONNÉES (ELT FLOW)
-- ===========================================================================

-- ---  ---
CREATE DATABASE IF NOT EXISTS AUTH_DB COMMENT = 'Données sensibles (Users, Hash Passwords)';
CREATE SCHEMA IF NOT EXISTS AUTH_DB.PROD;

CREATE TABLE IF NOT EXISTS AUTH_DB.PROD.USERS (
    EMAIL VARCHAR PRIMARY KEY,
    NOM_PRENOM VARCHAR NOT NULL,
    SERVICE VARCHAR NOT NULL,
    PASSWORD_HASH VARCHAR NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE DATABASE IF NOT EXISTS DOSSIERS_DB COMMENT = 'Métadonnées des enquêtes (Isolation logique)';
CREATE SCHEMA IF NOT EXISTS DOSSIERS_DB.PROD;

CREATE TABLE IF NOT EXISTS DOSSIERS_DB.PROD.DOSSIERS (
    DOSSIER_ID VARCHAR PRIMARY KEY,  -- UUID technique
    PV_NUMBER VARCHAR NOT NULL,      -- Référence Gendarmerie
    NOM_DOSSIER VARCHAR,
    TYPE_ENQUETE VARCHAR,
    DIRECTEUR_ENQUETE VARCHAR,
    DATE_SAISINE DATE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY_USER_EMAIL VARCHAR    -- Audit trail
);

-- Table de registre des fichiers importés
CREATE TABLE IF NOT EXISTS DOSSIERS_DB.PROD.FILES_LOG (
    FILE_ID VARCHAR(36) PRIMARY KEY,      -- UUID unique du fichier
    DOSSIER_ID VARCHAR(36) NOT NULL,      -- Lien vers l'enquête
    FILENAME VARCHAR(255),                -- Nom réel du fichier (ex: "mt20_orange.csv")
    FILE_TYPE VARCHAR(50),                -- 'MT20', 'MT24', 'HREF', 'ANNUAIRE'

    -- Métadonnées métier (Ce qu'on a saisi dans le formulaire)
    TARGET_NAME VARCHAR(100),             -- Abonné concerné
    TARGET_IDENTIFIER VARCHAR(50),        -- MSISDN ou IMEI ou ZONE_NAME

    -- Métadonnées techniques
    UPLOADED_BY VARCHAR(255),             -- Email de l'enquêteur
    UPLOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ROW_COUNT NUMBER                      -- Combien de lignes ingérées (Optionnel mais utile)
);

CREATE DATABASE IF NOT EXISTS RAW_DATA COMMENT = 'Landing Zone : Données brutes CSV (Schema-on-write)';
CREATE SCHEMA IF NOT EXISTS RAW_DATA.PNIJ_SRC;

-- ===========================================================================
-- TABLE MT20
-- ===========================================================================

CREATE TABLE IF NOT EXISTS RAW_DATA.PNIJ_SRC.RAW_MT20 (
    -- Colonnes Techniques (Ajoutées par Streamlit lors de l'upload)
    DOSSIER_ID VARCHAR,
    SOURCE_FILENAME VARCHAR,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Colonnes Métier (Tel que dans le CSV)
    "TYPE"                  VARCHAR,
    "DATE"                  VARCHAR,
    "CIBLE"                 VARCHAR,
    "CORRESPONDANT"         VARCHAR,
    "TYPE CORRESPONDANT"    VARCHAR,
    "DIRECTION"             VARCHAR,
    "DUREE"                 VARCHAR,
    "COMP."                 VARCHAR,
    "EFFICACITE"            VARCHAR,
    "IMSI"                  VARCHAR,
    "IMEI"                  VARCHAR,
    "CELLID"                VARCHAR,
    "ADRESSE IP VO WIFI"    VARCHAR,
    "PORT SOURCE VO WIFI"   VARCHAR,
    "ADRESSE2"              VARCHAR,
    "ADRESSE3"              VARCHAR,
    "ADRESSE4"              VARCHAR,
    "ADRESSE5"              VARCHAR,
    "CODE POSTAL"           VARCHAR,
    "VILLE"                 VARCHAR,
    "PAYS"                  VARCHAR,
    "TYPE-COORD"            VARCHAR,
    "X"                     VARCHAR,
    "Y"                     VARCHAR
);

-- ===========================================================================
-- TABLE MT24
-- ===========================================================================
CREATE TABLE IF NOT EXISTS RAW_DATA.PNIJ_SRC.RAW_MT24 (
    DOSSIER_ID VARCHAR,
    SOURCE_FILENAME VARCHAR,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    "TYPE"                  VARCHAR,
    "DATE"                  VARCHAR,
    "CIBLE"                 VARCHAR,
    "CORRESPONDANT"         VARCHAR,
    "TYPE CORRESPONDANT"    VARCHAR,
    "DIRECTION"             VARCHAR,
    "DUREE"                 VARCHAR,
    "COMP."                 VARCHAR,
    "EFFICACITE"            VARCHAR,
    "IMSI"                  VARCHAR,
    "IMEI"                  VARCHAR,
    "CELLID"                VARCHAR,
    "ADRESSE IP VO WIFI"    VARCHAR,
    "PORT SOURCE VO WIFI"   VARCHAR,
    "ADRESSE2"              VARCHAR,
    "ADRESSE3"              VARCHAR,
    "ADRESSE4"              VARCHAR,
    "ADRESSE5"              VARCHAR,
    "CODE POSTAL"           VARCHAR,
    "VILLE"                 VARCHAR,
    "PAYS"                  VARCHAR,
    "TYPE-COORD"            VARCHAR,
    "X"                     VARCHAR,
    "Y"                     VARCHAR
);

-- ===========================================================================
-- TABLE ANNUAIRE
-- ===========================================================================
CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE (
    -- 1. Métadonnées Techniques (Gérées par Streamlit)
    DOSSIER_ID VARCHAR(36),
    SOURCE_FILENAME VARCHAR(255),
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. Données Brutes du Fichier (Format CSV exact)
    -- Note : On utilise les guillemets "" car nos colonnes commencent par un _
    "_ficheNumero"                  VARCHAR,
    "_ficheTypeEquipement"          VARCHAR,
    "_ficheTypeNumero"              VARCHAR,
    "_ficheTypeTelephone"           VARCHAR,
    "_ficheOperateur"               VARCHAR,
    "_ficheDebutAbonnement"         VARCHAR,
    "_ficheFinAbonnement"           VARCHAR,
    "_ficheSource"                  VARCHAR,
    "_ficheTypeContrat"             VARCHAR,
    "_ficheContrat"                 VARCHAR,
    "_ficheOperateurContrat"        VARCHAR,
    "_ficheOptions"                 VARCHAR,
    "_ficheIMSI"                    VARCHAR,
    "_ficheIMEIvendu"               VARCHAR,
    "_ficheSIM"                     VARCHAR,
    "_personneType"                 VARCHAR,
    "_personneSource"               VARCHAR,
    "_personneNom"                  VARCHAR,
    "_personnePrenom"               VARCHAR,
    "_personneSurnom"               VARCHAR,
    "_personneRaisonSociale"        VARCHAR,
    "_personneAdresse"              VARCHAR,
    "_personneVille"                VARCHAR,
    "_personneCodePostal"           VARCHAR,
    "_personnePays"                 VARCHAR,
    "_personneCommentaire"          VARCHAR,
    "_utilisateurReelDateDebut"     VARCHAR,
    "_utilisateurReelDateFin"       VARCHAR,
    "_representantLegalSource"      VARCHAR,
    "_representantLegalNom"         VARCHAR,
    "_representantLegalPrenom"      VARCHAR,
    "_représentantLegalSurnom"      VARCHAR,
    "_representantLegalAdresse"     VARCHAR,
    "_representantLegalVille"       VARCHAR,
    "_representantLegalCodePostal"  VARCHAR,
    "_representantLegalPays"        VARCHAR,
    "_representantLegalCommentaire" VARCHAR
);

-- ===========================================================================
-- TABLE 1 HREF_ZONE_ORANGE - LES ÉVÉNEMENTS (COMMUNICATIONS)
-- ===========================================================================

CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE (
    -- 1. MÉTADONNÉES TECHNIQUES (Obligatoire)
    DOSSIER_ID          VARCHAR(36),  -- Lien vers l'enquête
    SOURCE_FILENAME     VARCHAR(255), -- Nom du fichier (ex: "orange_zone1.csv")
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE "ZONE" (CRUCIAL POUR LA CONFRONTATION)
    INPUT_ZONE_NAME     VARCHAR(100), -- Ex: "Braquage Banque"
    INPUT_ZONE_NUM      VARCHAR(10),  -- Ex: "1", "2", "A"... (Identifiant simple)
    INPUT_ZONE_CITY     VARCHAR(100), -- Ex: "Jons"
    INPUT_ZONE_LAT      VARCHAR(50),  -- Latitude cible définie par l'utilisateur
    INPUT_ZONE_LON      VARCHAR(50),  -- Longitude cible définie par l'utilisateur
    INPUT_START_DT      VARCHAR(50),  -- Début créneau demandé
    INPUT_END_DT        VARCHAR(50),  -- Fin créneau demandé

    -- 3. DONNÉES BRUTES FICHIER 1 (Format CSV exact)
    "Technologie"       VARCHAR,
    "Cellule"           VARCHAR,      -- Clé de jointure vers la table CELLS
    "IMSI"              VARCHAR,      -- Identifiant clé pour la confrontation
    "IMEI"              VARCHAR,      -- Identifiant clé pour la confrontation
    "MSISDN"            VARCHAR,      -- Identifiant clé pour la confrontation
    "Horodatage_debut"  VARCHAR,      -- ex: "2025-09-26 00:51:38"
    "TypeCDR"           VARCHAR,
    "SousTypeCDR"       VARCHAR
);

-- ===========================================================================
-- TABLE 2 HREF_ZONE_ORANGE: LE RÉFÉRENTIEL CELLULES (ANTENNES)
-- ===========================================================================
CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE (
    -- 1. MÉTADONNÉES TECHNIQUES
    DOSSIER_ID          VARCHAR(36),
    SOURCE_FILENAME     VARCHAR(255),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE (Pour lier aux events ci-dessus)
    INPUT_ZONE_NUM      VARCHAR(10),  -- Doit correspondre au numéro de zone du fichier 1

    -- 3. DONNÉES BRUTES FICHIER 2
    "Reseau"            VARCHAR,
    "CellID"            VARCHAR, -- Parfois avec espaces "20801 0704 380A"
    "CellID2"           VARCHAR, -- Version collée "208010704380A"
    "LACCISAC"          VARCHAR,
    "ECI"               VARCHAR,
    "Nom"               VARCHAR,
    "Adresse"           VARCHAR,
    "CP"                VARCHAR,
    "Ville"             VARCHAR,
    "Azimut"            VARCHAR,
    "X Lambert"         VARCHAR,
    "Y Lambert"         VARCHAR,
    "DateDebut"         VARCHAR,
    "DateFin"           VARCHAR
);

-- ===========================================================================
-- TABLE 1 HREF_ZONE_ORANGE - LES ÉVÉNEMENTS (COMMUNICATIONS)
-- ===========================================================================

CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE (
    -- 1. MÉTADONNÉES TECHNIQUES (Obligatoire)
    DOSSIER_ID          VARCHAR(36),  -- Lien vers l'enquête
    SOURCE_FILENAME     VARCHAR(255), -- Nom du fichier (ex: "orange_zone1.csv")
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE "ZONE" (CRUCIAL POUR LA CONFRONTATION)
    INPUT_ZONE_NAME     VARCHAR(100), -- Ex: "Braquage Banque"
    INPUT_ZONE_NUM      VARCHAR(10),  -- Ex: "1", "2", "A"... (Identifiant simple)
    INPUT_ZONE_CITY     VARCHAR(100), -- Ex: "Jons"
    INPUT_ZONE_LAT      VARCHAR(50),  -- Latitude cible définie par l'utilisateur
    INPUT_ZONE_LON      VARCHAR(50),  -- Longitude cible définie par l'utilisateur
    INPUT_START_DT      VARCHAR(50),  -- Début créneau demandé
    INPUT_END_DT        VARCHAR(50),  -- Fin créneau demandé

    -- 3. DONNÉES BRUTES FICHIER 1 (Format CSV exact)
    "Technologie"       VARCHAR,
    "Cellule"           VARCHAR,      -- Clé de jointure vers la table CELLS
    "IMSI"              VARCHAR,      -- Identifiant clé pour la confrontation
    "IMEI"              VARCHAR,      -- Identifiant clé pour la confrontation
    "MSISDN"            VARCHAR,      -- Identifiant clé pour la confrontation
    "Horodatage_debut"  VARCHAR,      -- ex: "2025-09-26 00:51:38"
    "TypeCDR"           VARCHAR,
    "SousTypeCDR"       VARCHAR
);

-- ===========================================================================
-- TABLE 2 HREF_ZONE_ORANGE: LE RÉFÉRENTIEL CELLULES (ANTENNES)
-- ===========================================================================
CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE (
    -- 1. MÉTADONNÉES TECHNIQUES
    DOSSIER_ID          VARCHAR(36),
    SOURCE_FILENAME     VARCHAR(255),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE (Pour lier aux events ci-dessus)
    INPUT_ZONE_NUM      VARCHAR(10),  -- Doit correspondre au numéro de zone du fichier 1

    -- 3. DONNÉES BRUTES FICHIER 2
    "Reseau"            VARCHAR,
    "CellID"            VARCHAR, -- Parfois avec espaces "20801 0704 380A"
    "CellID2"           VARCHAR, -- Version collée "208010704380A"
    "LACCISAC"          VARCHAR,
    "ECI"               VARCHAR,
    "Nom"               VARCHAR,
    "Adresse"           VARCHAR,
    "CP"                VARCHAR,
    "Ville"             VARCHAR,
    "Azimut"            VARCHAR,
    "X Lambert"         VARCHAR,
    "Y Lambert"         VARCHAR,
    "DateDebut"         VARCHAR,
    "DateFin"           VARCHAR
);

-- ===========================================================================
-- TABLE UNIQUE SFR (ÉVÉNEMENTS + CELLULES DÉNORMALISÉS)
-- ===========================================================================
CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_SFR (
    -- 1. MÉTADONNÉES TECHNIQUES
    DOSSIER_ID          VARCHAR(36),
    SOURCE_FILENAME     VARCHAR(255),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE "ZONE" (IDENTIQUE À ORANGE)
    INPUT_ZONE_NAME     VARCHAR(100),
    INPUT_ZONE_NUM      VARCHAR(10),  -- Le lien pour la confrontation (Ex: Zone "1")
    INPUT_ZONE_CITY     VARCHAR(100),
    INPUT_ZONE_LAT      VARCHAR(50),
    INPUT_ZONE_LON      VARCHAR(50),
    INPUT_START_DT      VARCHAR(50),
    INPUT_END_DT        VARCHAR(50),

    -- 3. DONNÉES BRUTES DU FICHIER SFR (Format CSV exact)
    -- Attention aux noms de colonnes longs et avec espaces/points -> Double Quotes obligatoires
    "Heure Eve. Reseau (heure de Paris)"                 VARCHAR,
    "Eve. Reseau"                                        VARCHAR,
    "IMSI"                                               VARCHAR,
    "MSISDN"                                             VARCHAR,
    "IMEI"                                               VARCHAR,
    "Marque Actuelle"                                    VARCHAR,
    "Modèle Actuel"                                      VARCHAR,
    "GCI"                                                VARCHAR, -- L'équivalent du CellID
    "Coordonnées GPS du barycentre de la couverture..."  VARCHAR, -- Contient "Lat,Lon" dans une seule case
    "Rayon théorique de la couverture de la cellule"     VARCHAR,
    "Adresse postale implantation du Site"               VARCHAR,
    "Code Postal implantation du Site"                   VARCHAR,
    "Ville implantation du Site"                         VARCHAR,
    "Longitude adresse du Site (Lambert2)"               VARCHAR,
    "Latitude adresse du Site (Lambert2)"                VARCHAR,
    "IP du Hotspot VoWIFI"                               VARCHAR,
    "Coordonnées GPS du hotspot VoWIFI"                  VARCHAR,
    "Rayon théorique Hotspot VoWIFI"                     VARCHAR
);

-- ===========================================================================
-- TABLE UNIQUE BOUYGUES (HREF ZONES)
-- ===========================================================================
CREATE OR REPLACE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES (
    -- 1. MÉTADONNÉES TECHNIQUES
    DOSSIER_ID          VARCHAR(36),
    SOURCE_FILENAME     VARCHAR(255),
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- 2. MÉTADONNÉES DU FORMULAIRE "ZONE" (Standardisé pour tous les opérateurs)
    INPUT_ZONE_NAME     VARCHAR(100),
    INPUT_ZONE_NUM      VARCHAR(10),  -- Clé de confrontation
    INPUT_ZONE_CITY     VARCHAR(100),
    INPUT_ZONE_LAT      VARCHAR(50),
    INPUT_ZONE_LON      VARCHAR(50),
    INPUT_START_DT      VARCHAR(50),
    INPUT_END_DT        VARCHAR(50),

    -- 3. DONNÉES BRUTES DU FICHIER BOUYGUES
    -- On respecte strictement l'en-tête du CSV fourni
    "Event.StartTime"           VARCHAR, -- Ex: "06/10/2025 15:15:00"
    "Event.IMSI"                VARCHAR,
    "Event.MSISDN"              VARCHAR,
    "Event.IMEI"                VARCHAR,
    "Adresse1.Numero"           VARCHAR,
    "Adresse2.Voie-Lieu"        VARCHAR,
    "Adresse3.CodePostal"       VARCHAR,
    "Adresse4.Ville"            VARCHAR,
    "Event.Code"                VARCHAR,
    "Event.Libl_Fr"             VARCHAR,
    "Event.Country"             VARCHAR,
    "Network"                   VARCHAR,
    "Cell.Techno"               VARCHAR,
    "Cell.Name"                 VARCHAR,
    "Cell.eCGI"                 VARCHAR, -- L'identifiant unique de l'antenne
    "Cell.W84X"                 VARCHAR, -- Attention : format avec virgule "45,798188"
    "Cell.W84Y"                 VARCHAR, -- Attention : format avec virgule "5,466991"
    "Event.Libl_En"             VARCHAR,
    "Event.Type"                VARCHAR,
    "Heure de Event.StartTime"  VARCHAR, -- Nom avec espaces -> guillemets obligatoires
    "Mobile.Manufacturer"       VARCHAR,
    "Mobile.Type"               VARCHAR,
    "Max. Cell.Azimut"          VARCHAR
);

-- --- C. Zones de Transformation (dbt) ---
CREATE DATABASE IF NOT EXISTS STAGING COMMENT = 'Zone de travail dbt (Nettoyage, Casting, Déduplication)';
CREATE SCHEMA IF NOT EXISTS STAGING.DBT_STAGING;

CREATE DATABASE IF NOT EXISTS MARTS COMMENT = 'Zone de consommation (Tables finales pour l''UI)';
CREATE SCHEMA IF NOT EXISTS MARTS.PROD;

-- ===========================================================================
-- 4. ATTRIBUTION DES PRIVILÈGES (GRANT)
-- ===========================================================================
-- Note : On utilise SECURITYADMIN ou ACCOUNTADMIN pour les grants, ou le propriétaire des objets.
-- Ici on suppose qu'on est toujours SYSADMIN (propriétaire) ou ACCOUNTADMIN.

-- --- A. Privilèges pour STREAMLIT_ROLE ---

-- 1. Accès au Compute
GRANT USAGE ON WAREHOUSE STREAMLIT_WH TO ROLE STREAMLIT_ROLE;

-- 2. Accès Auth & Dossiers (Lecture/Ecriture)
GRANT USAGE ON DATABASE AUTH_DB TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA AUTH_DB.PROD TO ROLE STREAMLIT_ROLE;
GRANT SELECT, INSERT ON TABLE AUTH_DB.PROD.USERS TO ROLE STREAMLIT_ROLE; -- Login + Inscription potentielle

GRANT USAGE ON DATABASE DOSSIERS_DB TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA DOSSIERS_DB.PROD TO ROLE STREAMLIT_ROLE;
GRANT SELECT, INSERT ON TABLE DOSSIERS_DB.PROD.DOSSIERS TO ROLE STREAMLIT_ROLE;
GRANT SELECT, INSERT ON TABLE DOSSIERS_DB.PROD.FILES_LOG TO ROLE STREAMLIT_ROLE;

-- 3. Accès RAW (Ingestion)
GRANT USAGE ON DATABASE RAW_DATA TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA RAW_DATA.PNIJ_SRC TO ROLE STREAMLIT_ROLE;
-- Ajout de SELECT nécessaire pour la méthode 'write_pandas' de Snowpark
GRANT SELECT, INSERT ON TABLE RAW_DATA.PNIJ_SRC.RAW_MT20 TO ROLE STREAMLIT_ROLE;
GRANT SELECT, INSERT ON TABLE RAW_DATA.PNIJ_SRC.RAW_MT24 TO ROLE STREAMLIT_ROLE;
GRANT INSERT, SELECT ON TABLE RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE TO ROLE STREAMLIT_ROLE;
GRANT INSERT, SELECT ON TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE TO ROLE STREAMLIT_ROLE;
GRANT INSERT, SELECT ON TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE TO ROLE STREAMLIT_ROLE;
GRANT INSERT, SELECT ON TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_SFR TO ROLE STREAMLIT_ROLE;
GRANT INSERT, SELECT ON TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES TO ROLE STREAMLIT_ROLE;
-- 4. Accès MARTS (Visualisation)
GRANT USAGE ON DATABASE MARTS TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA MARTS.PROD TO ROLE STREAMLIT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARTS.PROD TO ROLE STREAMLIT_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MARTS.PROD TO ROLE STREAMLIT_ROLE;

-- --- B. Privilèges pour DBT_ROLE ---

-- 1. Accès au Compute
GRANT USAGE ON WAREHOUSE DBT_WH TO ROLE DBT_ROLE;

-- 2. Accès Sources (Lecture Seule)
GRANT USAGE ON DATABASE RAW_DATA TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA RAW_DATA.PNIJ_SRC TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DATA.PNIJ_SRC TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_DATA.PNIJ_SRC TO ROLE DBT_ROLE;

GRANT USAGE ON DATABASE DOSSIERS_DB TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA DOSSIERS_DB.PROD TO ROLE DBT_ROLE;
GRANT SELECT ON TABLE DOSSIERS_DB.PROD.DOSSIERS TO ROLE DBT_ROLE;

-- 3. Accès Transformation (Plein Pouvoir sur Staging et Marts)
GRANT USAGE ON DATABASE STAGING TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA STAGING.DBT_STAGING TO ROLE DBT_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA STAGING.DBT_STAGING TO ROLE DBT_ROLE;

GRANT USAGE ON DATABASE MARTS TO ROLE DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA MARTS.PROD TO ROLE DBT_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA MARTS.PROD TO ROLE DBT_ROLE;