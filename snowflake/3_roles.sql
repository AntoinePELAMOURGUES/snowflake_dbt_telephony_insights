-- Création des rôles
CREATE ROLE TRANSFORMER;
CREATE ROLE ORCHESTRATOR;
CREATE ROLE APP_USER;
CREATE ROLE APP_OWNER;
CREATE ROLE RAW_DATA_READER;

-- Attribution des privilèges
GRANT ALL ON DATABASE tel_insights_raw TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE tel_insights_analytics TO ROLE ORCHESTRATOR;

-- Droits de navigation
GRANT USAGE ON DATABASE tel_insights_analytics TO ROLE APP_USER;
GRANT USAGE ON SCHEMA tel_insights_analytics.event_curated TO ROLE APP_USER;

-- Droits de lecture sur tous les objets actuels
GRANT SELECT ON ALL TABLES IN SCHEMA tel_insights_analytics.event_curated TO ROLE APP_USER;
GRANT SELECT ON ALL VIEWS IN SCHEMA tel_insights_analytics.event_curated TO ROLE APP_USER;

-- Droits de lecture sur les objets créés à l’avenir
GRANT SELECT ON FUTURE TABLES IN SCHEMA tel_insights_analytics.event_curated TO ROLE APP_USER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA tel_insights_analytics.event_curated TO ROLE APP_USER;


GRANT OWNERSHIP ON SCHEMA tel_insights_analytics.event_curated TO ROLE APP_OWNER REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA tel_insights_analytics.users_judiciaires TO ROLE APP_OWNER REVOKE CURRENT GRANTS;
GRANT USAGE ON WAREHOUSE TEL_INSIGHTS_ANALYTICS TO ROLE APP_OWNER;

GRANT USAGE ON DATABASE tel_insights_raw TO ROLE RAW_DATA_READER;
GRANT SELECT ON FUTURE TABLES IN DATABASE tel_insights_raw TO ROLE RAW_DATA_READER;

-- Attribution des rôles aux utilisateurs
GRANT ROLE TRANSFORMER TO USER dev_analyst;
GRANT ROLE ORCHESTRATOR TO USER prod_orchestrator;
GRANT ROLE APP_USER TO USER enqueteur;
GRANT ROLE APP_OWNER TO USER admin_app;
GRANT ROLE RAW_DATA_READER TO USER enqueteur;
