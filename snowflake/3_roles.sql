-- Création des rôles
CREATE ROLE IF NOT EXISTS TRANSFORMER;
CREATE ROLE IF NOT EXISTS ORCHESTRATOR;
CREATE ROLE IF NOT EXISTS APP_USER;
CREATE ROLE IF NOT EXISTS APP_OWNER;
CREATE ROLE IF NOT EXISTS RAW_DATA_READER;

-- Attribution des privilèges
GRANT ALL ON DATABASE raw TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE analytics_dev TO ROLE ORCHESTRATOR;
GRANT ALL ON DATABASE analytics_dev TO ROLE sysadmin;


-- Droits de navigation
GRANT USAGE ON DATABASE analytics_dev TO ROLE APP_USER;

GRANT USAGE ON SCHEMA analytics_dev.stg TO ROLE APP_USER;
GRANT USAGE ON SCHEMA analytics_dev.marts TO ROLE APP_USER;

-- Droits de lecture sur tous les objets actuels
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_dev.marts TO ROLE APP_USER;
GRANT SELECT ON ALL VIEWS IN SCHEMA analytics_dev.marts TO ROLE APP_USER;

-- Droits de lecture sur les objets créés à l’avenir
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics_dev.marts TO ROLE APP_USER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA analytics_dev.marts TO ROLE APP_USER;


GRANT OWNERSHIP ON SCHEMA analytics_dev.marts TO ROLE APP_OWNER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA analytics_dev.stg TO ROLE APP_OWNER REVOKE CURRENT GRANTS;
GRANT USAGE ON WAREHOUSE TEL_INSIGHTS_ANALYTICS TO ROLE APP_OWNER;

GRANT USAGE ON DATABASE raw TO ROLE RAW_DATA_READER;
GRANT SELECT ON FUTURE TABLES IN DATABASE raw TO ROLE RAW_DATA_READER;

use role accountadmin;
-- 1. Donner les droits de LECTURE sur la couche RAW
-- (Nécessaire pour que le SELECT de la vue fonctionne)
GRANT USAGE ON DATABASE RAW TO ROLE sysadmin;
GRANT USAGE ON SCHEMA RAW.CSV TO ROLE sysadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW.CSV TO ROLE sysadmin;
-- (Optionnel mais recommandé) Permet de lire les futures tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW.CSV TO ROLE sysadmin;

-- 2. Donner les droits d'ECRITURE sur la couche STAGING
-- (C'est ce qui cause l'erreur)
GRANT USAGE ON DATABASE analytics_dev TO ROLE sysadmin;
GRANT USAGE ON SCHEMA analytics_dev.stg TO ROLE sysadmin; -- (Si 'STG' est le schéma)
GRANT CREATE VIEW ON SCHEMA analytics_dev.stg TO ROLE sysadmin;
GRANT CREATE TABLE ON SCHEMA analytics_dev.stg TO ROLE sysadmin;
GRANT CREATE PROCEDURE ON SCHEMA analytics_dev.stg TO ROLE sysadmin;

GRANT USAGE ON WAREHOUSE TEL_INSIGHTS_ETL TO ROLE sysadmin;
--- Attribution des rôles aux utilisateurs
-- GRANT ROLE TRANSFORMER TO USER dev_analyst;
-- GRANT ROLE ORCHESTRATOR TO USER prod_orchestrator;
-- GRANT ROLE APP_USER TO USER enqueteur;
-- GRANT ROLE APP_OWNER TO USER admin_app;
-- GRANT ROLE RAW_DATA_READER TO USER enqueteur;
