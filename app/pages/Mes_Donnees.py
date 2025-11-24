import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import bcrypt
import re
import pandas as pd
import uuid
from datetime import datetime
from streamlit_option_menu import option_menu
import time
from modules.delete_file_data import delete_file_data

# Au début de votre fichier (ex: Gestion_Dossiers.py)
from modules.utils import trigger_airflow_pipeline


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
    options=["Mes Fichiers", "Intégrer des Données", "Supprimer des Données"],
    icons=["database-check", "database-add", "trash"],
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
            DOSSIER_ID,
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

        st.divider()

        # D. Section ANNUAIRE
        st.subheader("📒 Annuaire")

        # On filtre sur le type ANNUAIRE défini dans la table FILES_LOG
        df_annuaire = df_logs[df_logs["FILE_TYPE"] == "ANNUAIRE"]

        if not df_annuaire.empty:
            # On récupère la date la plus récente (max) de la colonne UPLOADED_AT
            last_upload_dt = df_annuaire["UPLOADED_AT"].max()

            # Création de colonnes pour une mise en page propre
            col_annuaire_1, col_annuaire_2 = st.columns([1, 3])

            with col_annuaire_1:
                # Affichage sous forme de métrique pour un impact visuel immédiat
                st.metric(
                    label="Dernière mise à jour",
                    value=last_upload_dt.strftime("%d/%m/%Y"),
                    delta=last_upload_dt.strftime("%H:%M"),
                    delta_color="off",  # Gris neutre
                )

            with col_annuaire_2:
                st.info(
                    f"L'annuaire pour ce dossier est actif. "
                    f"Dernier fichier intégré : **{df_annuaire.iloc[0]['FILENAME']}** "
                    f"par {df_annuaire.iloc[0]['UPLOADED_BY']}."
                )

        else:
            st.caption("⚠️ Aucun annuaire n'est actuellement associé à ce dossier.")

        st.markdown("---")
        st.markdown(
            f"""
            <h1 style='text-align: center; color: #0055A4; font-size: 40px;'>
                🚀 Actions & Analyses
            </h1>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("---")

        # --- 1. PRÉPARATION INTELLIGENTE DE LA LISTE DE CHOIX ---
        if not df_logs.empty:
            # A. Séparation : On isole l'Annuaire du reste
            df_annuaire = df_logs[df_logs["FILE_TYPE"] == "ANNUAIRE"].copy()
            df_others = df_logs[df_logs["FILE_TYPE"] != "ANNUAIRE"].copy()

            # B. Filtrage Annuaire : On ne garde que le plus récent (Top 1)
            if not df_annuaire.empty:
                df_annuaire = df_annuaire.sort_values(
                    by="UPLOADED_AT", ascending=False
                ).head(1)

            # C. Reconstruction : On recolle les morceaux
            df_choices = pd.concat([df_others, df_annuaire], ignore_index=True)

            # On retrie le tout par date récente
            df_choices = df_choices.sort_values(by="UPLOADED_AT", ascending=False)

            # D. Création du Label pour le menu déroulant
            df_choices["DISPLAY_LABEL"] = df_choices.apply(
                lambda x: f"{x['FILE_TYPE']} - {x['TARGET_IDENTIFIER']} ({x['TARGET_NAME']})",
                axis=1,
            )
            options_files = df_choices["DISPLAY_LABEL"].tolist()
        else:
            df_choices = pd.DataFrame()
            options_files = []

        # --- 2. ZONE D'ACTION (ANALYSE) ---
        # On utilise un container pour bien grouper visuellement cette partie
        with st.container():
            # A. Sélection des fichiers
            selected_files_labels = st.multiselect(
                "Sélectionnez les éléments à analyser ou confronter :",
                options=options_files,
                placeholder="Choisissez 1 ou plusieurs fichiers (MT20, MT24, Zone...)",
            )

        # B. Bouton d'action (Dynamique)
        if selected_files_labels:
            count = len(selected_files_labels)

            # Définition de la destination
            if count == 1:
                btn_label = "🔍 Lancer l'Analyse Individuelle"
                target_page = "pages/Analyse_Individuelle.py"
                help_text = "Génère le rapport complet pour l'élément sélectionné."
            else:
                btn_label = f"⚔️ Lancer la Confrontation ({count} éléments)"
                target_page = "pages/Confrontation.py"
                help_text = (
                    "Compare les éléments entre eux (Interactions, Zones communes...)."
                )

            # On met le bouton un peu en évidence avec des colonnes pour centrer ou ajuster la largeur si besoin
            c1, c2, c3 = st.columns([1, 2, 1])
            with c2:
                if st.button(
                    btn_label, type="primary", use_container_width=True, help=help_text
                ):
                    # Sauvegarde du contexte
                    selected_rows = df_choices[
                        df_choices["DISPLAY_LABEL"].isin(selected_files_labels)
                    ]
                    st.session_state["analysis_context"] = selected_rows.to_dict(
                        "records"
                    )

                    # Redirection
                    st.switch_page(target_page)
                else:
                    st.info(
                        "👆 Veuillez sélectionner au moins un fichier ci-dessus pour débloquer les outils d'analyse."
                    )

        # --- 3. PIED DE PAGE (NAVIGATION) ---
        st.markdown("---")  # Séparateur visuel fort
        st.caption("Navigation")

if st.button("⬅️ Retour à la liste des dossiers"):
    st.switch_page("pages/Gestion_Dossiers.py")

# ==============================================================================
# ONGLET 2 : FORMULAIRE D'INGESTION
# ==============================================================================
if selected_tab == "Intégrer des Données":
    st.header("Intégrer de nouvelles données")

    data_type = st.selectbox(
        "Type de Réquisition",
        ["MT20 (Ligne)", "MT24 (Boîtier)", "HREF (Zone/Antennes)", "Annuaire"],
    )

    st.divider()

    with st.form("upload_form", clear_on_submit=True):
        # Init des variables
        target_name = ""
        target_identifier = ""
        source_filename = ""
        table_target = ""
        short_type = ""

        col_op, col_dummy = st.columns([1, 1])
        operator = col_op.selectbox(
            "Opérateur (Requis pour le nommage)", ["ORANGE", "SFR", "BOUYGUES", "FREE"]
        )
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
            source_filename = f"MT20_{operator}_{target_identifier}"
            st.info("ℹ️ Le fichier doit être un CSV standard PNIJ.")

        elif "MT24" in data_type:
            target_name = st.text_input("Nom de l'Abonné / Cible").strip().upper()
            target_identifier = cols[0].text_input("Numéro IMEI", placeholder="3545...")
            source_filename = f"MT24_{operator}_{target_identifier}"

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

        elif "Annuaire" in data_type:
            # On fixe des valeurs génériques pour l'annuaire car il concerne tout le dossier
            target_name = "ANNUAIRE_DOSSIER"
            target_identifier = "GLOBAL"

            # Message d'information adapté au format réel de la table RAW_ANNUAIRE
            st.info(
                "ℹ️ Le fichier attendu est un export PNIJ standard (CSV). "
                "Il doit contenir les colonnes techniques commençant par '_' "
                "(ex: '_ficheNumero', '_personneNom', ...)."
            )

        uploaded_files = st.file_uploader(
            "Sélectionner les fichiers CSV",
            accept_multiple_files=True,
            type=["csv", "txt"],
        )
        submitted = st.form_submit_button("🚀 Lancer l'ingestion")

        if submitted and uploaded_files:
            progress_bar = st.progress(0)

            # Définition des colonnes strictes pour l'Annuaire (Protection contre les formats exotiques)
            annuaire_snowflake_columns = [
                "_ficheNumero",
                "_ficheTypeEquipement",
                "_ficheTypeNumero",
                "_ficheTypeTelephone",
                "_ficheOperateur",
                "_ficheDebutAbonnement",
                "_ficheFinAbonnement",
                "_ficheSource",
                "_ficheTypeContrat",
                "_ficheContrat",
                "_ficheOperateurContrat",
                "_ficheOptions",
                "_ficheIMSI",
                "_ficheIMEIvendu",
                "_ficheSIM",
                "_personneType",
                "_personneSource",
                "_personneNom",
                "_personnePrenom",
                "_personneSurnom",
                "_personneRaisonSociale",
                "_personneAdresse",
                "_personneVille",
                "_personneCodePostal",
                "_personnePays",
                "_personneCommentaire",
                "_utilisateurReelDateDebut",
                "_utilisateurReelDateFin",
                "_representantLegalSource",
                "_representantLegalNom",
                "_representantLegalPrenom",
                "_représentantLegalSurnom",
                "_representantLegalAdresse",
                "_representantLegalVille",
                "_representantLegalCodePostal",
                "_representantLegalPays",
                "_representantLegalCommentaire",
            ]

            for idx, file in enumerate(uploaded_files):
                try:
                    # 1. Lecture Pandas (Gestion encodage & Séparateur)
                    # On lit tout en string pour ne pas perdre les '0' devant les numéros
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

                    # Fallback : Si le séparateur n'était pas ';' (ex: fichier CSV simple colonne), on tente la virgule
                    if df.shape[1] < 2:
                        file.seek(0)
                        try:
                            df = pd.read_csv(
                                file,
                                sep=",",
                                dtype=str,
                                on_bad_lines="skip",
                                encoding="utf-8",
                            )
                        except:
                            pass  # On garde la version précédente si ça échoue

                    # 2. Aiguillage & Logique spécifique
                    # On détermine la cible AVANT d'enrichir pour adapter le filtrage
                    quote_identifiers_flag = (
                        False  # Par défaut False, sauf pour Annuaire
                    )
                    # Détermination du tag selon le type de fichier choisi
                    dbt_tag_to_run = "all"  # Par défaut

                    if "MT20" in data_type:
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT20"
                        short_type = "MT20"
                        # Pour MT20, source_filename est déjà défini par l'input utilisateur (target_identifier)
                        quote_identifiers_flag = True
                        dbt_tag_to_run = "communications"

                    elif "MT24" in data_type:
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_MT24"
                        short_type = "MT24"
                        # Pour MT24, source_filename est déjà défini par l'input utilisateur
                        quote_identifiers_flag = True
                        dbt_tag_to_run = "communications"

                    elif "HREF" in data_type:
                        # Pour HREF, on garde les tags de zone car c'est structurel pour le croisement
                        df["INPUT_ZONE_NUM"] = target_identifier
                        short_type = "HREF"
                        # Source filename défini par l'input utilisateur
                        quote_identifiers_flag = True
                        dbt_tag_to_run = "reseau"

                        cols_list = df.columns.tolist()
                        if any("Heure Eve" in c for c in cols_list):
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_SFR"
                            quote_identifiers_flag = (
                                True  # SFR a des espaces dans les colonnes
                            )
                            source_filename = (
                                f"HREF_SFR_ZONE_{target_identifier}_{ville}_{date_href}"
                            )
                        elif "Technologie" in cols_list and "Cellule" in cols_list:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE"
                            quote_identifiers_flag = True
                            source_filename = f"HREF_ORANGE_COMS_ZONE_{target_identifier}_{ville}_{date_href}"
                        elif "X Lambert" in cols_list or "CellID" in cols_list:
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE"
                            quote_identifiers_flag = True
                            source_filename = f"HREF_ORANGE_CELLS_ZONE_{target_identifier}_{ville}_{date_href}"
                        elif (
                            "Event.StartTime" in cols_list or "Cell.Techno" in cols_list
                        ):
                            table_target = "RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES"
                            quote_identifiers_flag = True
                            source_filename = f"HREF_BOUYGUES_ZONE_{target_identifier}_{ville}_{date_href}"
                        else:
                            st.error(f"❌ Format HREF non reconnu pour : {file.name}")
                            continue

                    elif "Annuaire" in data_type:
                        table_target = "RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE"
                        short_type = "ANNUAIRE"
                        source_filename = (
                            file.name
                        )  # IMPORTANT: On garde le vrai nom du fichier pour l'annuaire
                        quote_identifiers_flag = (
                            True  # Obligatoire car les colonnes commencent par "_"
                        )
                        dbt_tag_to_run = "annuaire"

                        # --- FILTRAGE STRICT ANNUAIRE ---
                        # On ne garde que les colonnes qui existent vraiment dans Snowflake
                        valid_cols = [
                            c for c in df.columns if c in annuaire_snowflake_columns
                        ]
                        if not valid_cols:
                            st.warning(
                                f"⚠️ Ignoré : Aucune colonne PNIJ valide trouvée dans {file.name}"
                            )
                            continue
                        df = df[valid_cols]

                    # 3. Enrichissement Standard (Après filtrage pour ne pas perdre ces colonnes)
                    df["DOSSIER_ID"] = dossier_id
                    df["SOURCE_FILENAME"] = source_filename

                    # 4. Écriture dans RAW (Données)
                    if table_target:
                        session.write_pandas(
                            df,
                            table_name=table_target.split(".")[-1],
                            database="RAW_DATA",
                            schema="PNIJ_SRC",
                            auto_create_table=False,
                            overwrite=False,
                            quote_identifiers=quote_identifiers_flag,  # Gestion fine des guillemets
                        )

                        # 5. Écriture dans LOGS
                        # Conversion explicite en string pour éviter les soucis de type UUID
                        row_count = len(df)

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
                                    "ROW_COUNT": row_count,
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
                    st.error(
                        f"❌ Erreur technique sur le fichier {file.name}: {str(e)}"
                    )

                progress_bar.progress((idx + 1) / len(uploaded_files))

            # =================================================================
            # ### AJOUT : DÉCLENCHEMENT DE L'ORCHESTRATION (AIRFLOW)
            # =================================================================
            # On déclenche seulement si au moins un fichier a été traité
            # et on le fait UNE SEULE FOIS pour tout le lot.
            st.info(f"🔄 Déclenchement du traitement pour : {dbt_tag_to_run}")
            trigger_airflow_pipeline(target_tag=dbt_tag_to_run)
            # =================================================================

            st.success("✅ Ingestion terminée !")
            time.sleep(2)  # Petit temps de pause pour voir la barre à 100%
            st.rerun()  # Rafraîchissement pour mettre à jour l'onglet "Mes Fichiers"

# ==============================================================================
# ONGLET 3 : SUPPRESSION DE DONNÉES
# ==============================================================================
if selected_tab == "Supprimer des Données":
    st.header("🗑️ Suppression de Fichiers")
    st.warning(
        "⚠️ Attention : La suppression est définitive. Les données seront retirées des tables brutes et des journaux."
    )

    # Récupération des fichiers disponibles
    query_logs_del = f"""
        SELECT FILE_ID, FILENAME, FILE_TYPE, TARGET_NAME, UPLOADED_AT
        FROM DOSSIERS_DB.PROD.FILES_LOG
        WHERE DOSSIER_ID = '{dossier_id}'
        ORDER BY UPLOADED_AT DESC
    """
    df_del = session.sql(query_logs_del).to_pandas()

    if df_del.empty:
        st.info("Aucun fichier à supprimer dans ce dossier.")
    else:
        # Création d'une étiquette lisible pour le Selectbox
        # Ex: "[MT20] fichier.csv (Cible: DUPONT) - 21/11/2025"
        df_del["LABEL"] = df_del.apply(
            lambda x: f"[{x['FILE_TYPE']}] {x['FILENAME']} (Cible: {x['TARGET_NAME']}) - {x['UPLOADED_AT'].strftime('%d/%m %H:%M')}",
            axis=1,
        )

        # Formulaire de suppression
        with st.form("delete_form"):
            selected_label = st.selectbox(
                "Sélectionnez le fichier à supprimer :",
                options=df_del["LABEL"].tolist(),
            )

            # Case à cocher de sécurité (Optionnel mais recommandé)
            confirm_check = st.checkbox(
                "Je confirme vouloir supprimer définitivement ces données."
            )

            btn_delete = st.form_submit_button(
                "🚨 Supprimer le fichier", type="primary"
            )

            if btn_delete:
                if not confirm_check:
                    st.error("Veuillez cocher la case de confirmation.")
                else:
                    # On retrouve les infos techniques (ID, Filename) à partir du Label sélectionné
                    file_info = df_del[df_del["LABEL"] == selected_label].iloc[0]

                    with st.spinner("Suppression en cours..."):
                        success = delete_file_data(
                            session,
                            file_id=file_info["FILE_ID"],
                            file_type=file_info["FILE_TYPE"],
                            filename=file_info["FILENAME"],
                            dossier_id=dossier_id,
                        )

                        if success:
                            st.success(
                                f"✅ Le fichier {file_info['FILENAME']} a été supprimé avec succès."
                            )
                            time.sleep(1.5)
                            st.rerun()
