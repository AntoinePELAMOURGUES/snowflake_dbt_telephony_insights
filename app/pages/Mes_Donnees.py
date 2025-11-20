import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import bcrypt
import re
import pandas as pd
import uuid
from datetime import datetime
from streamlit_option_menu import option_menu


# --- 1. RÉCUPÉRATION DE LA SESSION SNOWFLAKE ---
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
#  INTERFACE
# ==============================================================================

st.markdown(
    f"""
    <h1 style='text-align: center; color: #0055A4; font-size: 40px;'>
        🗃️ Gestion des Données d'Enquête
    </h1>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# --- MENU DE NAVIGATION ---
selected_tab = option_menu(
    menu_title=None,
    options=["Mes Fichiers", "Intégrer des Données"],
    icons=["database-check", "database-add"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

# Récupération du contexte
dossier_id = st.session_state.get("current_dossier_id", "DOSSIER_TEST_123")
user_email = st.session_state.get("user_email", "enqueteur@gendarmerie.fr")

# ==============================================================================
# ONGLET 1 : VISUALISATION DES FICHIERS
# ==============================================================================
if selected_tab == "Mes Fichiers":
    # Utilisation d'une CTE ou LIMIT pour optimiser l'affichage si nécessaire
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
                        "ROW_COUNT",
                    ]
                ],
                column_config={
                    "TARGET_NAME": "NOM_ABONNE",
                    "TARGET_IDENTIFIER": "MSISDN",
                    "UPLOADED_AT": st.column_config.DatetimeColumn(
                        "DATE_INGESTION", format="DD/MM/YYYY, HH:mm"
                    ),
                    "ROW_COUNT": "NOMBRE_LIGNES",
                    "UPLOADED_BY": "CHARGE_PAR",
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
                        "ROW_COUNT",
                    ]
                ],
                column_config={
                    "TARGET_NAME": "NOM_ABONNE",
                    "TARGET_IDENTIFIER": "IMEI",
                    "UPLOADED_AT": st.column_config.DatetimeColumn(
                        "DATE_INGESTION", format="DD/MM/YYYY, HH:mm"
                    ),
                    "ROW_COUNT": "NOMBRE_LIGNES",
                    "UPLOADED_BY": "CHARGE_PAR",
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
            st.dataframe(
                df_href[
                    [
                        "TARGET_IDENTIFIER",
                        "FILENAME",
                        "TARGET_NAME",
                        "UPLOADED_AT",
                        "UPLOADED_BY",
                        "ROW_COUNT",
                    ]
                ],
                column_config={
                    "TARGET_IDENTIFIER": "ID_ZONE",
                    "TARGET_NAME": "NOM_ZONE",
                    "UPLOADED_AT": st.column_config.DatetimeColumn(
                        "DATE_INGESTION", format="DD/MM/YYYY, HH:mm"
                    ),
                    "ROW_COUNT": "NOMBRE_LIGNES",
                    "UPLOADED_BY": "CHARGE_PAR",
                },
                width="stretch",
                hide_index=True,
            )

# ==============================================================================
# ONGLET 2 : FORMULAIRE D'INGESTION
# ==============================================================================
if selected_tab == "Intégrer des Données":
    st.header("Intégrer de nouvelles données")

    data_type = st.selectbox(
        "Type de Réquisition",
        ["MT20 (Ligne)", "MT24 (Boîtier)", "HREF (Zone/Antennes)", "Annuaire / Fiches"],
    )

    st.divider()

    with st.form("upload_form", clear_on_submit=True):
        # Init des variables
        target_name = ""
        target_identifier = ""
        source_filename = ""
        table_target = ""
        short_type = ""

        cols = st.columns(2)

        # --- LOGIQUE DYNAMIQUE ---
        if "MT20" in data_type:
            target_name = (
                st.text_input("Nom de l'Abonné / Cible", placeholder="Ex: John Doe")
                .strip()
                .upper()
            )
            target_identifier = cols[0].text_input(
                "Numéro de Ligne (MSISDN)", placeholder="336xxxxxxxx"
            )
            source_filename = f"MT20_{target_identifier}"
            st.info("ℹ️ Le fichier doit être un CSV standard PNIJ.")

        elif "MT24" in data_type:
            target_name = st.text_input("Nom de l'Abonné / Cible").strip().upper()
            target_identifier = cols[0].text_input("Numéro IMEI", placeholder="3545...")
            source_filename = f"MT24_{target_identifier}"

        elif "HREF" in data_type:
            target_name = (
                st.text_input("Nom de la Zone", placeholder="Ex: Braquage Agence")
                .strip()
                .upper()
            )
            target_identifier = cols[0].text_input(
                "Numéro/ID Zone", placeholder="Ex: 1"
            )
            ville = cols[1].text_input("Ville de la zone").strip().upper()

            c1, c2 = st.columns(2)
            date_href = c1.date_input("Date des faits", value=datetime.today())
            source_filename = f"HREF_ZONE_{target_identifier}_{ville}_{date_href}"

            st.warning("Chargez TOUS les fichiers de la zone en même temps.")

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
                    # 1. Lecture Pandas (Gestion encodage)
                    try:
                        df = pd.read_csv(
                            file,
                            sep=";",
                            dtype=str,
                            on_bad_lines="skip",
                            encoding="utf-8",
                        )
                    except UnicodeDecodeError:
                        file.seek(0)
                        df = pd.read_csv(
                            file,
                            sep=";",
                            dtype=str,
                            on_bad_lines="skip",
                            encoding="latin-1",
                        )

                    # 2. Enrichissement Standard
                    df["DOSSIER_ID"] = dossier_id
                    # ICI : On utilise bien votre variable calculée pour le nom du fichier
                    df["SOURCE_FILENAME"] = source_filename

                    # 3. Aiguillage & Logique spécifique
                    if "MT20" in data_type:
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT20"
                        short_type = "MT20"

                    elif "MT24" in data_type:
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT24"
                        short_type = "MT24"

                    elif "HREF" in data_type:
                        # Pour HREF, on garde les tags de zone car c'est structurel pour le croisement
                        df["INPUT_ZONE_NAME"] = target_name
                        df["INPUT_ZONE_NUM"] = target_identifier
                        df["INPUT_ZONE_CITY"] = ville
                        short_type = "HREF"

                        cols_list = df.columns.tolist()
                        if any("Heure Eve" in c for c in cols_list):
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_SFR"
                        elif "Technologie" in cols_list and "Cellule" in cols_list:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE"
                        elif "X Lambert" in cols_list or "CellID" in cols_list:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE"
                        elif (
                            "Event.StartTime" in cols_list or "Cell.Techno" in cols_list
                        ):
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES"
                        else:
                            st.error(f"Format HREF non reconnu : {file.name}")
                            continue

                    # 4. Écriture dans RAW (Données)
                    if table_target:
                        session.write_pandas(
                            df,
                            table_name=table_target.split(".")[-1],
                            database="RAW_DATA",
                            schema="PNIJ_SRC",
                            auto_create_table=False,
                            overwrite=False,
                        )

                        # 5. Écriture dans LOGS
                        log_entry = pd.DataFrame(
                            [
                                {
                                    "FILE_ID": str(uuid.uuid4()),
                                    "DOSSIER_ID": dossier_id,
                                    "FILENAME": source_filename,
                                    "FILE_TYPE": short_type,
                                    "TARGET_NAME": target_name,
                                    "TARGET_IDENTIFIER": target_identifier,
                                    "UPLOADED_BY": user_email,
                                    "ROW_COUNT": len(df),
                                }
                            ]
                        )

                        # Utilisation des arguments NOMMÉS (keyword arguments) obligatoire ici
                        session.write_pandas(
                            log_entry,
                            table_name="FILES_LOG",
                            database="DOSSIERS_DB",  # Argument nommé
                            schema="PROD",  # Argument nommé
                            auto_create_table=False,
                            overwrite=False,
                        )

                except Exception as e:
                    st.error(f"Erreur sur le fichier {file.name}: {e}")

                progress_bar.progress((idx + 1) / len(uploaded_files))

            st.success("✅ Ingestion terminée !")
            st.balloons()
