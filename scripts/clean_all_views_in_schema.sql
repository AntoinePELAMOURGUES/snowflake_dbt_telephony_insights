-- Remplace 'TON_SCHEMA' par le nom réel du schema (ex: DBT_STAGING)
use role SYSADMIN;
use database STAGING;

EXECUTE IMMEDIATE $$
DECLARE
    -- 1. On liste toutes les vues du schéma
    c1 CURSOR FOR SELECT table_name FROM INFORMATION_SCHEMA.VIEWS WHERE table_schema = 'DBT_STAGING';
    view_name VARCHAR;
    cmd VARCHAR;
BEGIN
    -- 2. On boucle sur chaque vue trouvée
    FOR record IN c1 DO
        view_name := record.table_name;
        -- 3. On construit la commande DROP
        cmd := 'DROP VIEW IF EXISTS DBT_STAGING.' || view_name;
        -- 4. On l'exécute
        EXECUTE IMMEDIATE cmd;
    END FOR;
    RETURN 'Toutes les vues ont été supprimées avec succès.';
END;
$$;