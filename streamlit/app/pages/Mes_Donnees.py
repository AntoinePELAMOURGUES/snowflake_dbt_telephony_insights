import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import bcrypt
import re
import pandas as pd
import uuid
from datetime import datetime
from streamlit_option_menu import option_menu


# --- 1. RÉCUPÉRATION DE LA SESSION SNOWFLAKE (via le cache) ---
# --- 1. RÉCUPÉRATION DE LA SESSION SNOWFLAKE (via le cache) ---
@st.cache_resource
def create_snowpark_session():
    try:
        connection_parameters = st.secrets["snowflake"]
        session = Session.builder.configs(connection_parameters).create()
        return session
    except Exception as e:
        st.error(f"Erreur de connexion à Snowflake : {e}")
        return None


session = create_snowpark_session()

if not session:
    st.error("Connexion à Snowflake échouée.")
    st.stop()

# --- 2. Vérification de l'Authentification ---
if not st.session_state.get("is_logged_in", False):
    st.error("Accès refusé. Veuillez vous authentifier.")
    st.page_link("pages/Authentification.py", label="Retour à l'authentification")
    st.stop()


# ==============================================================================
#  ZONE DE DÉFINITION DES FONCTIONS (GLOBALES)
# ==============================================================================

st.markdown(
    f"""
    <h1 style='text-align: center; color: #0055A4; font-size: 40px;'>
        🗃️ Gestion des Données d'Enquête <span style='color: #0055A4'></span>
    </h1>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# --- 4. MENU DE NAVIGATION ---
selected_tab = option_menu(
    menu_title=None,
    options=[
        "Mes Fichiers",
        "Intégrer des Données",
    ],
    icons=["database-check", "database-add"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

# Récupération du contexte (Simulé pour l'exemple)
dossier_id = st.session_state.get("current_dossier_id", "DOSSIER_TEST_123")
user_email = st.session_state.get("user_email", "enqueteur@gendarmerie.fr")

# ==============================================================================
# ONGLET 1 : VISUALISATION DES FICHIERS INTÉGRÉS
# ==============================================================================

if selected_tab == "Mes Fichiers":
    st.header(f"Fichiers du dossier en cours")

    # 1. Récupération du registre des fichiers depuis Snowflake
    # On ne scanne pas les tables RAW, mais la table de LOG rapide.
    query_logs = f"""
        SELECT
            FILE_TYPE,
            FILENAME,
            TARGET_NAME,
            TARGET_IDENTIFIER,
            UPLOADED_AT,
            UPLOADED_BY,
            ROW_COUNT
        FROM DOSSIERS_DB.PROD.FILES_LOG
        WHERE DOSSIER_ID = '{dossier_id}'
        ORDER BY UPLOADED_AT DESC
    """
    df_logs = session.sql(query_logs).to_pandas()

    if df_logs.empty:
        st.info("Aucun fichier n'a encore été intégré dans ce dossier.")
    else:
        # A. Section MT20
        st.subheader("📞 Mes fichiers de type MT20 (Lignes)")
        df_mt20 = df_logs[df_logs["FILE_TYPE"] == "MT20"]
        if not df_mt20.empty:
            st.dataframe(
                df_mt20[
                    [
                        "TARGET_NAME",
                        "TARGET_IDENTIFIER",
                        "FILENAME",
                        "UPLOADED_AT",
                        "UPLOADED_BY",
                    ]
                ],
                column_config={
                    "TARGET_IDENTIFIER": "MSISDN",
                    "UPLOADED_AT": st.column_config.DatetimeColumn(
                        "Date d'ingestion", format="D MMM YYYY, HH:mm"
                    ),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("Aucune réquisition MT20.")

        st.divider()

        # B. Section MT24
        st.subheader("📱 Mes fichiers de type MT24 (Boîtiers)")
        df_mt24 = df_logs[df_logs["FILE_TYPE"] == "MT24"]
        if not df_mt24.empty:
            st.dataframe(
                df_mt24[
                    [
                        "TARGET_NAME",
                        "TARGET_IDENTIFIER",
                        "FILENAME",
                        "UPLOADED_AT",
                        "UPLOADED_BY",
                    ]
                ],
                column_config={
                    "TARGET_IDENTIFIER": "IMEI",
                    "UPLOADED_AT": st.column_config.DatetimeColumn(
                        "Date d'ingestion", format="D MMM YYYY, HH:mm"
                    ),
                },
                width="stretch",
                hide_index=True,
            )
        else:
            st.caption("Aucune réquisition MT24.")

        st.divider()

        # C. Section HREF / Zones
        st.subheader("📡 Mes Zones (HREF)")
        df_href = df_logs[df_logs["FILE_TYPE"].str.contains("HREF")]
        if not df_href.empty:
            st.dataframe(df_href, width="stretch", hide_index=True)

# ==============================================================================
# ONGLET 2 : FORMULAIRE D'INGESTION
# ==============================================================================
with tab_upload:
    st.header("Intégrer de nouvelles données")

    with st.form("upload_form", clear_on_submit=True):
        # 1. Choix du Type
        data_type = st.selectbox(
            "Type de Réquisition",
            [
                "MT20 (Ligne)",
                "MT24 (Boîtier)",
                "HREF (Zone/Antennes)",
                "Annuaire / Fiches",
            ],
        )

        # 2. Champs Dynamiques selon le type
        target_name = st.text_input(
            "Nom de l'Abonné / Cible",
            placeholder="Ex: John Doe ou 'Suspect Braquage'",
        )
        target_identifier = ""  # Variable pour stocker MSISDN, IMEI ou Nom Zone

        cols = st.columns(2)

        # Logique conditionnelle d'affichage
        if "MT20" in data_type:
            target_identifier = cols[0].text_input(
                "Numéro de Ligne (MSISDN)", placeholder="336xxxxxxxx"
            )
            st.info("ℹ️ Le fichier doit être un CSV standard PNIJ (Orange, SFR...).")

        elif "MT24" in data_type:
            target_identifier = cols[0].text_input("Numéro IMEI", placeholder="3545...")

        elif "HREF" in data_type:
            # Champs spécifiques HREF Zone
            target_identifier = cols[0].text_input(
                "Nom/Numéro de la Zone",
                help="Identifiant pour grouper les fichiers (ex: 'Zone 1')",
            )
            cols[1].text_input("Ville de la zone")
            c1, c2 = st.columns(2)
            lat = c1.text_input("Latitude (Optionnel)")
            lon = c2.text_input("Longitude (Optionnel)")
            st.warning(
                "Pour HREF, chargez les fichiers (Events + Cellules) en une fois si possible."
            )

        # 3. Upload Fichier(s)
        uploaded_files = st.file_uploader(
            "Sélectionner les fichiers CSV",
            accept_multiple_files=True,
            type=["csv", "txt"],
        )

        submitted = st.form_submit_button("🚀 Lancer l'ingestion")

        if submitted and uploaded_files:
            progress_bar = st.progress(0)

            for idx, file in enumerate(uploaded_files):
                try:
                    # --- A. Lecture Pandas ---
                    # On lit tout en string pour éviter les erreurs de type, Snowflake gérera
                    df = pd.read_csv(file, sep=";", dtype=str, on_bad_lines="skip")

                    # --- B. Enrichissement (Ajout des métadonnées pour RAW) ---
                    # Colonnes techniques
                    df["DOSSIER_ID"] = dossier_id
                    df["SOURCE_FILENAME"] = file.name

                    # Colonnes Formulaire (Mappées vers les INPUT_... de Snowflake)
                    if "MT20" in data_type:
                        df["INPUT_TARGET_NAME"] = target_name
                        df["INPUT_MSISDN_TARGET"] = target_identifier
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT20"
                        short_type = "MT20"

                    elif "MT24" in data_type:
                        df["INPUT_TARGET_NAME"] = target_name
                        df["INPUT_IMEI_TARGET"] = target_identifier
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT24"
                        short_type = "MT24"

                    elif "HREF" in data_type:
                        # Logique simple : on envoie vers SFR ou ORANGE selon le nom ou une détection auto
                        # Pour l'exemple, on simplifie. Idéalement, on détecte les colonnes.
                        df["INPUT_ZONE_NAME"] = target_name
                        df["INPUT_ZONE_NUM"] = target_identifier
                        short_type = "HREF"

                        # Détection basique (A affiner)
                        if "Heure Eve. Reseau" in df.columns:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_SFR"
                        elif "Cellule" in df.columns and "Technologie" in df.columns:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE"
                        else:
                            # Fallback ou erreur
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES"

                    # --- C. Ecriture dans RAW (Snowflake) ---
                    session.write_pandas(
                        df,
                        table_name=table_target.split(".")[-1],  # Juste le nom de table
                        database="RAW_DATA",
                        schema="PNIJ_SRC",
                        auto_create_table=False,
                        overwrite=False,
                    )

                    # --- D. Mise à jour du Registre (FILES_LOG) ---
                    # C'est l'étape clé pour que l'onglet 1 fonctionne !
                    log_entry = pd.DataFrame(
                        [
                            {
                                "FILE_ID": str(uuid.uuid4()),
                                "DOSSIER_ID": dossier_id,
                                "FILENAME": file.name,
                                "FILE_TYPE": short_type,
                                "TARGET_NAME": target_name,
                                "TARGET_IDENTIFIER": target_identifier,
                                "UPLOADED_BY": user_email,
                                "UPLOADED_AT": datetime.now(),
                                "ROW_COUNT": len(df),
                            }
                        ]
                    )

                    session.write_pandas(
                        log_entry,
                        table_name="FILES_LOG",
                        database="DOSSIERS_DB",
                        schema="PROD",
                        auto_create_table=False,
                        overwrite=False,
                    )

                except Exception as e:
                    st.error(f"Erreur sur le fichier {file.name}: {str(e)}")

                # Mise à jour barre de progression
                progress_bar.progress((idx + 1) / len(uploaded_files))

            st.success(
                "✅ Ingestion terminée ! Les données sont en cours de traitement par le Data Warehouse."
            )
            # Ici on pourrait déclencher l'API Airflow
            st.balloons()
