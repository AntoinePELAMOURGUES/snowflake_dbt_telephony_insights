{{ config(
    materialized='table',
    cluster_by=['dossier_id', 'numero_zone', 'date_heure_utc_fr']
) }}

WITH unified_href AS (
    SELECT * FROM {{ ref('int_href_unified') }}
)

SELECT
    -- On reprend tout, propre et net
    dossier_id,
    source_filename,
    operateur,

    -- La clé de l'enquête : La Zone (1, 2, A, B...)
    numero_zone,

    date_heure_utc_fr,

    -- Les cibles
    imsi,
    imei,
    msisdn,

    -- Le contexte
    description_event, -- "APPEL SORTANT" ou "LOCATION UPDATE"

    -- La Géolocalisation prête pour st.map()
    latitude,
    longitude,

    -- Infos complémentaires
    ville_antenne as ville_cellule,
    adresse_cellule

FROM unified_href