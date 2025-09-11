select
Event_StartTime as GDH,
Event_IMSI as IMSI,
Event_MSISDN as MSISDN,
Event_IMEI as IMEI,
upper(Adresse2_VoieLieu || ' ' || Adresse3_CodePostal || ' ' || Adresse4_Ville) as ADRESSE_RELAIS,
Event_Libl_Fr as LIBELLE,
Cell_W84X as LATITUDE,
Cell_W84Y as LONGITUDE,
upper(Mobile_Type) as MODELE_TPH,
'zone_2' as ZONE
from TEL_INSIGHTS.RAW.BOUYGUES_ZONE_2