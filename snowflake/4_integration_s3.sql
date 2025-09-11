-- INTEGRATION - FILE FORMAT - CREATION STAGE

------------------------------------------------------------------------------------------------

-- Création de l'intégration vers un stockage S3 après configuration AWS (politiques et rôle IAM)
CREATE STORAGE INTEGRATION tel_insights_s3
  TYPE = EXTERNAL_STAGE                     -- On crée une intégration de type "stage externe"
  STORAGE_PROVIDER = 'S3'                   -- Le provider de stockage est Amazon S3
  ENABLED = TRUE                            -- L'intégration est active
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::317249460587:role/snowflake_role' -- ARN du rôle IAM créé côté AWS
  STORAGE_ALLOWED_LOCATIONS = ('s3://tel-insights-s3/raw/'); -- Autorise la connexion à ce chemin S3 uniquement

-- Affichage des détails de l'intégration, notamment l'utilisateur AWS IAM généré par Snowflake à utiliser dans la relation de confiance IAM
DESC INTEGRATION tel_insights_s3;

-- Attribution de droits pour que le rôle SYSADMIN puisse créer un stage (zone de préparation externe) dans le schéma event_raw
GRANT CREATE STAGE ON SCHEMA event_raw TO ROLE SYSADMIN;

-- Attribution du droit d'utilisation de l'intégration à SYSADMIN
GRANT USAGE ON INTEGRATION tel_insights_s3 TO ROLE SYSADMIN;

-- Bascule sur le rôle sysadmin - obligatoire pour la gestion des objets du schéma ciblé
USE ROLE sysadmin;

-- Sélection du schéma où sera créé le stage externe
USE SCHEMA event_raw;

-- Définition d'un format de fichier CSV adapté aux fichiers attendus du S3
CREATE FILE FORMAT mon_format_csv
  TYPE = 'CSV'
  FIELD_DELIMITER = ';'                        -- Délimiteur de champ : point-virgule
  SKIP_HEADER = 1                              -- Saut du header (première ligne)
  NULL_IF = ('', 'NULL')                       -- Traite '' ou 'NULL' comme des valeurs nulles
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'           -- Les champs peuvent être encadrés par des guillemets
  ENCODING = 'UTF8'                            -- Encodage du fichier
  DATE_FORMAT = 'DD/MM/YYYY'                   -- Format de date
  TIMESTAMP_FORMAT = 'DD/MM/YYYY HH24:MI:SS';  -- Format de timestamp

-- Création d'une zone de préparation externe (stage) associée à l'intégration S3 et au format de fichier défini
CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = tel_insights_s3
  URL = 's3://tel-insights-s3/raw/'
  FILE_FORMAT = mon_format_csv;

-- Affichage du contenu du répertoire S3 lié au stage
LIST @my_s3_stage;

-- Lecture des deux premières colonnes du fichier CSV “Evenements Zone 2_data.csv” situé dans S3, avec le format défini, affichage limité à 10 lignes
SELECT $1, $2
FROM '@my_s3_stage/Evenements Zone 2_data.csv' (FILE_FORMAT => 'mon_format_csv')
LIMIT 10;
