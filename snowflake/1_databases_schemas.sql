-- On se positionne sur le rôle sysadmin pour la création d’objets globaux et de schémas
USE ROLE sysadmin;

-- Création des deux databases principales :
-- 1 - "raw" pour réceptionner les données sources non modifiées
-- 2 - "analytics" pour stocker les vues, objets enrichis et servir la restitution (Streamlit)
CREATE DATABASE tel_insights_raw;
CREATE DATABASE tel_insights_analytics;

-- Création des schémas dans RAW (organisation par source et sujet)
-- event_raw : pour tous les événements bruts (par opérateur, zone)
-- location_roaming : données de localisation et roaming brutes
CREATE SCHEMA tel_insights_raw.event_raw;
CREATE SCHEMA tel_insights_raw.location_roaming;

-- Création des schémas dans ANALYTICS
-- event_curated : événements transformés, structurés pour l’analyse
-- users_judiciaires : profils métiers, accès spécifiques pour les enquêteurs
CREATE SCHEMA tel_insights_analytics.event_curated;
CREATE SCHEMA tel_insights_analytics.users_judiciaires;

-- Suppression éventuelle du schéma par défaut "public" pour forcer le passage par la gouvernance métier
DROP SCHEMA IF EXISTS tel_insights_raw.public;
DROP SCHEMA IF EXISTS tel_insights_analytics.public;

-- Attribution des droits d’usage à SYSADMIN sur toutes les bases et schémas créés
GRANT USAGE ON DATABASE tel_insights_raw TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE tel_insights_analytics TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA tel_insights_raw.event_raw TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA tel_insights_raw.location_roaming TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA tel_insights_analytics.event_curated TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA tel_insights_analytics.users_judiciaires TO ROLE SYSADMIN;

-- Droits spécifiques sur la partie analytique pour permettre création de dashboards Streamlit et de zones de staging
GRANT CREATE STREAMLIT ON SCHEMA tel_insights_analytics.event_curated TO ROLE SYSADMIN;
GRANT CREATE STREAMLIT ON SCHEMA tel_insights_analytics.users_judiciaires TO ROLE SYSADMIN;
GRANT CREATE STAGE ON SCHEMA tel_insights_analytics.event_curated TO ROLE SYSADMIN;
GRANT CREATE STAGE ON SCHEMA tel_insights_analytics.users_judiciaires TO ROLE SYSADMIN;
