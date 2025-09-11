-- Création d'un warehouse dédié aux opérations ETL (import, nettoyage, transformation)
-- Taille initiale recommandée : "M" (moyenne) pour la volumétrie standard, avec possibilité d’upscaling temporaire si besoin
CREATE WAREHOUSE tel_insights_etl
  WAREHOUSE_SIZE = 'XSMALL'                -- Taille "M" adaptée au chargement de fichiers classiques
  AUTO_SUSPEND = 60                        -- Suspendre automatiquement le warehouse après 60s d'inactivité pour optimiser les coûts
  AUTO_RESUME = TRUE                       -- Redémarrage automatique à la demande
  INITIALLY_SUSPENDED = TRUE;              -- Ne démarre pas tant qu'aucune requête n'est lancée

-- Création d'un warehouse pour l’usage analytique (requêtes Streamlit, BI, extraction de rapports, visualisation)
-- Taille adaptée à l’analyse croisée multi-zones : "M" par défaut, "L" ou "XL" en cas de pics d’activité envisagés
CREATE WAREHOUSE tel_insights_analytics
  WAREHOUSE_SIZE = 'XSMALL'                -- Taille "M" par défaut, facilement modifiable selon l'activité
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Attribution des droits d’utilisation sur les warehouses créés au rôle SYSADMIN ou au(x) rôle(s) enquêteurs concernés
GRANT USAGE ON WAREHOUSE tel_insights_etl TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE tel_insights_analytics TO ROLE SYSADMIN;

-- En cas de volumétrie exceptionnelle (gros imports simultanés, analyses croisées massives), le dimensionnement peut être temporairement augmenté
-- Exemple de commande pour scaler vers "L" ou "XL" :
-- ALTER WAREHOUSE tel_insights_etl SET WAREHOUSE_SIZE = 'LARGE';
-- ALTER WAREHOUSE tel_insights_analytics SET WAREHOUSE_SIZE = 'XLARGE';
