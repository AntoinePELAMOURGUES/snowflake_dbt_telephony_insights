/*
================================================================
SCRIPT DE CONFIGURATION SNOWFLAKE
POUR PROJET TELEPHONY INSIGHTS (STREAMLIT)
================================================================
Rôle requis : ACCOUNTADMIN
*/

-- Définit le contexte d'exécution
USE ROLE ACCOUNTADMIN;

-- 1. Création de l'Entrepôt (Warehouse)
CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60 -- (secondes) S'arrête après 1 min d'inactivité
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Entrepôt dédié à l''application Streamlit PROJET TELEPHONY INSIGHTS';

-- 2. Création de la Base de Données d'Authentification
CREATE DATABASE IF NOT EXISTS AUTH_DB
  COMMENT = 'Base de données pour l''authentification des utilisateurs de l''application Streamlit';

-- 3. Création du Schéma de Production
CREATE SCHEMA IF NOT EXISTS AUTH_DB.PROD
  COMMENT = 'Schéma de production pour les tables d''authentification';

-- 4. Création du Rôle Applicatif (Droits minimums)
CREATE ROLE IF NOT EXISTS STREAMLIT_ROLE
  COMMENT = 'Rôle applicatif pour l''application Streamlit.';

-- 5. Création de l'Utilisateur de Service (le "compte de service")
CREATE USER IF NOT EXISTS STREAMLIT_APP_USER
  -- [SÉCURITÉ] Remplacez ceci par le mot de passe réel de votre secrets.toml
  PASSWORD = 'MOT_DE_PASSE_TRES_SECURISE'
  -- [IMPORTANT] Lie les objets par défaut à l'utilisateur
  DEFAULT_ROLE = STREAMLIT_ROLE
  DEFAULT_WAREHOUSE = STREAMLIT_WH
  DEFAULT_NAMESPACE = AUTH_DB.PROD
  -- Empêche la demande de changement de mdp à la première connexion
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Utilisateur de service pour la connexion Streamlit';

-- 6. Liaison Utilisateur <-> Rôle
GRANT ROLE STREAMLIT_ROLE TO USER STREAMLIT_APP_USER;

-- 7. Attribution des Permissions au Rôle
-- Le rôle a besoin de "voir" et "utiliser" les objets
GRANT USAGE ON WAREHOUSE STREAMLIT_WH TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON DATABASE AUTH_DB TO ROLE STREAMLIT_ROLE;
GRANT USAGE ON SCHEMA AUTH_DB.PROD TO ROLE STREAMLIT_ROLE;

-- 8. Création de la Table d'Authentification
-- (Conforme au script Python de la page d'authentification)
USE SCHEMA AUTH_DB.PROD;
CREATE TABLE IF NOT EXISTS USERS (
  EMAIL VARCHAR(255) PRIMARY KEY, -- Clé primaire
  PASSWORD_HASH VARCHAR(255) NOT NULL, -- Stocke le hash bcrypt
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 9. Permissions sur la Table
-- Le rôle doit pouvoir LIRE (SELECT) la table USERS pour vérifier le hash
GRANT SELECT ON TABLE AUTH_DB.PROD.USERS TO ROLE STREAMLIT_ROLE;

-- Confirmation
SHOW GRANTS TO ROLE STREAMLIT_ROLE;
SHOW GRANTS TO USER STREAMLIT_APP_USER;

/*
================================================================
Fin du Script de Configuration
================================================================
*/