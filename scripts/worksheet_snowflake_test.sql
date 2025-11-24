-- ============================================================================
-- 🛠️ WORKSHEET DE DIAGNOSTIC & NETTOYAGE - PROJET TELEPHONY INSIGHTS
-- ============================================================================
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. AUTH & ADMINISTRATION (Dossiers, Utilisateurs)
-- ============================================================================
-- Vérifier les utilisateurs inscrits
SELECT * FROM AUTH_DB.PROD.USERS LIMIT 10;

-- Vérifier les dossiers d'enquête actifs
SELECT * FROM DOSSIERS_DB.PROD.DOSSIERS LIMIT 10;

-- Vérifier l'historique des fichiers chargés (Logs d'ingestion)
SELECT * FROM DOSSIERS_DB.PROD.FILES_LOG ORDER BY UPLOADED_AT DESC LIMIT 20;


-- ============================================================================
-- 2. COUCHE RAW (Données Brutes)
-- Vérifier si l'ingestion Streamlit a bien fonctionné
-- ============================================================================
-- Communications (MT20 / MT24)
SELECT 'RAW_MT20' as source, count(*) as nb_lignes FROM RAW_DATA.PNIJ_SRC.RAW_MT20
UNION ALL
SELECT 'RAW_MT24', count(*) FROM RAW_DATA.PNIJ_SRC.RAW_MT24;

-- Aperçu MT20 (Lignes)
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_MT20 LIMIT 10;
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_MT20
WHERE SOURCE_FILENAME LIKE '%FREE%'
LIMIT 10;-- Aperçu MT24 (Boîtiers)
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_MT24 LIMIT 10;
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_MT24
WHERE SOURCE_FILENAME LIKE '%BOUYGUES%'
LIMIT 10;-- Aperçu MT24 (Boîtiers)
-- Annuaire
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE LIMIT 10;

-- Données Antennes (HREF)
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES LIMIT 5;
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_HREF_SFR LIMIT 5;
SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE LIMIT 5;


-- ============================================================================
-- 3. COUCHE STAGING (Données Nettoyées via dbt)
-- Vérifier si tes modèles dbt 'stg' transforment bien les données
-- ============================================================================
-- Vérifier le parsing des dates et la géolocalisation
SELECT * FROM STAGING.DBT_STAGING.STG_MT_BOUYGUES LIMIT 20;
SELECT * FROM STAGING.DBT_STAGING.STG_MT_FREE LIMIT 20;

SELECT * FROM STAGING.DBT_STAGING.STG_MT_ORANGE LIMIT 20;
SELECT * FROM STAGING.DBT_STAGING.STG_MT_SFR LIMIT 20;

-- Vérifier l'unification des événements antennes
SELECT * FROM STAGING.DBT_STAGING.STG_HREF_EVENTS LIMIT 20;

-- Vérifier l'annuaire propre
SELECT * FROM STAGING.DBT_STAGING.STG_ANNUAIRE LIMIT 10;
SELECT * FROM RAW_DATA.PNIJ_SRC.INT_COMMUNICATIONS_ALL LIMIT 20;

-- ============================================================================
-- 4. COUCHE MARTS (Données Finales pour Streamlit)
-- C'est ce que ton appli Streamlit interroge pour les graphiques
-- ============================================================================
-- Table unifiée (MT20 + MT24)
SELECT * FROM MARTS.PROD.FCT_COMMUNICATIONS
ORDER BY DATE_HEURE_UTC_FR DESC
LIMIT 50;

-- Analyse des patterns (si tu as ce modèle)
-- SELECT * FROM MARTS.PROD.DIM_PATTERNS_VIE LIMIT 10;


-- ============================================================================
-- 🚨 ZONE DE DANGER : RESET / TRUNCATE
-- Décommenter les lignes uniquement pour vider les tables
-- ============================================================================

-- 1. Vider les Logs (Attention, l'appli ne saura plus quels fichiers sont chargés)
TRUNCATE TABLE DOSSIERS_DB.PROD.FILES_LOG;

-- 2. Vider les Données Brutes (RAW)
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_MT20;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_MT24;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_SFR;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE;
TRUNCATE TABLE RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE;

-- 3. Vider les utilisateurs (Attention, tu ne pourras plus te connecter)
-- TRUNCATE TABLE AUTH_DB.PROD.USERS;

drop view raw_data.pnij_src.int_communications_all;

select * from  STAGING.DBT_STAGING.int_communications_all where operateur='SFR' limit 100;

SELECT * FROM RAW_DATA.PNIJ_SRC.RAW_MT20 LIMIT 10;

select * from staging.intermediate.int_annuaire;

select * from marts.prod.fct_communications limit 10;

select * from marts.prod.fct_bornage_zones limit 50;

select * from staging.intermediate.int_href_unified limit 50;

select * from staging.dbt_staging.stg_href_sfr limit 50;
select * from staging.dbt_staging.stg_href_orange limit 50;

select * from raw_data.pnij_src.raw_href_events_orange limit 50;

select * from staging.dbt_staging.stg_href_bouygues limit 50;

SHOW GRANTS TO USER DBT_USER;
-- (Remplacez DBT_USER par le login trouvé à l'étape 1 si c'est différent)

use role accountadmin;

-- 1. Débloquer l'utilisateur
ALTER USER DBT_USER SET MINS_TO_UNLOCK = 0;

-- 2. (Vivement conseillé) Réinitialiser le mot de passe pour être sûr à 100%
-- Choisissez un mot de passe simple le temps du test, sans caractères trop exotiques
ALTER USER DBT_USER SET PASSWORD = 'UnMotDePasseSuperFort123!';

USE DATABASE STAGING;
USE SCHEMA DBT_STAGING;

-- Suppression des vues "bloquées" pour repartir de zéro
DROP VIEW IF EXISTS STG_HREF_ORANGE;
DROP VIEW IF EXISTS STG_HREF_BOUYGUES;
DROP VIEW IF EXISTS STG_HREF_SFR;
DROP VIEW IF EXISTS STG_MT_ORANGE;
DROP VIEW IF EXISTS STG_MT_SFR;
DROP VIEW IF EXISTS STG_MT_FREE;
DROP VIEW IF EXISTS STG_MT_BOUYGUES;
DROP VIEW IF EXISTS STG_ANNUAIRE;

USE DATABASE MARTS;
USE SCHEMA PROD;

-- On supprime les tables qui bloquent (elles seront recréées par dbt)
DROP TABLE IF EXISTS FCT_COMMUNICATIONS;
DROP TABLE IF EXISTS FCT_BORNAGE_ZONES;
DROP TABLE IF EXISTS DIM_DOSSIERS; -- Au cas où elle existe encore

-- Vérification de sécurité : On s'assure que DBT_ROLE a bien les droits sur le schéma
GRANT ALL PRIVILEGES ON SCHEMA MARTS.PROD TO ROLE DBT_ROLE;
GRANT CREATE TABLE ON SCHEMA MARTS.PROD TO ROLE DBT_ROLE;