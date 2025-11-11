-- On se positionne sur le rôle sysadmin pour la création d’objets globaux et de schémas
USE ROLE sysadmin;

-- Création des deux databases principales :
-- 1 - "raw" pour réceptionner les données sources non modifiées
-- 2 - "analytics" pour stocker les vues, objets enrichis et servir la restitution (Streamlit)
CREATE DATABASE IF NOT EXISTS raW;
CREATE DATABASE IF NOT EXISTS analytics_dev;

CREATE SCHEMA IF NOT EXISTS raw.csv;

-- Création des schémas dans ANALYTICS
-- event_curated : événements transformés, structurés pour l’analyse
-- users_judiciaires : profils métiers, accès spécifiques pour les enquêteurs
CREATE SCHEMA if not exists analytics_dev.stg;
CREATE SCHEMA if not exists analytics_dev.marts;

-- Suppression éventuelle du schéma par défaut "public" pour forcer le passage par la gouvernance métier
DROP SCHEMA IF EXISTS raw.public;
DROP SCHEMA IF EXISTS analytics_dev.public;

-- Attribution des droits d’usage à SYSADMIN sur toutes les bases et schémas créés
GRANT USAGE ON DATABASE analytics_dev TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE raw TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA raw.csv TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA analytics_dev.stg TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA analytics_dev.marts TO ROLE SYSADMIN;


-- Droits spécifiques sur la partie analytique pour permettre création de dashboards Streamlit et de zones de staging
GRANT CREATE STREAMLIT ON SCHEMA raw.csv TO ROLE SYSADMIN;
GRANT CREATE STREAMLIT ON SCHEMA analytics_dev.stg TO ROLE SYSADMIN;
GRANT CREATE STAGE ON SCHEMA analytics_dev.marts TO ROLE SYSADMIN;
