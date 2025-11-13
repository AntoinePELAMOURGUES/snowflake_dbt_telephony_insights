-- Assurez-vous d'être dans le bon contexte
USE ROLE ACCOUNTADMIN;
USE SCHEMA AUTH_DB.PROD;

-- Insérez votre premier utilisateur (ex: admin de l'application)
INSERT INTO USERS (EMAIL, PASSWORD_HASH)
VALUES (
  'antoine.pelamourgues@gendarmerie.interieur.gouv.fr',
  -- Collez le hash bcrypt généré par le script Python ici
  '$2b$12$QTXJX9q.gw3ErY5WSAhseOmhztKHdkAxtJW8Gjn0VRlNScOq6sMrO'
);

-- Vérification
SELECT * FROM USERS;