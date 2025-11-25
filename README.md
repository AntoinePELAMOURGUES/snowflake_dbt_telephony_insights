# 📱 Telephony Insights

![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit)
![Snowflake](https://img.shields.io/badge/Data_Warehouse-Snowflake-29B5E8?style=for-the-badge&logo=snowflake)
![dbt](https://img.shields.io/badge/Transformation-dbt-FF694B?style=for-the-badge&logo=dbt)
![Python](https://img.shields.io/badge/Language-Python-3776AB?style=for-the-badge&logo=python)

> **Solution d'intelligence analytique pour l'exploitation des données de téléphonie judiciaire.**

---

## 🔍 Le Projet

**Telephony Insights** est une application conçue pour assister les enquêteurs de police judiciaire dans l'analyse des fadettes (facturations détaillées) et des données de bornage. Face à l'hétérogénéité des formats opérateurs (Orange, SFR, Bouygues, Free), cet outil centralise, normalise et visualise les communications pour transformer des fichiers CSV bruts en renseignement actionnable.

### 🎯 Fonctionnalités Clés

* **Ingestion Universelle :** Support des réquisitions **MT20** (Cible Ligne) et **MT24** (Cible IMEI) multi-opérateurs.
* **Isolation "Dossier" :** Architecture Multi-Tenant garantissant l'étanchéité stricte des données entre deux enquêtes via un `DOSSIER_ID` unique.
* **Cartographie Interactive :** Projection des événements réseaux sur carte (Folium/Mapbox) via le référentiel des antennes relais.
* **Analyse Relationnelle :** Détection automatique des liens **SIM ↔ Boîtier** (Qui utilise quel téléphone ?).

---

## 🏗 Architecture Technique (Modern Data Stack)

L'application repose sur une architecture **ELT (Extract, Load, Transform)** cloud-native, privilégiant la sécurité et la performance.

```mermaid
graph LR
    User((Enquêteur)) -->|Upload CSV| Streamlit
    Streamlit -->|Load| Snowflake[(Snowflake RAW)]
    Snowflake -->|Transform| dbt(dbt Core)
    dbt -->|Model| Marts[(Data Marts)]
    Marts -->|Visualize| Streamlit
