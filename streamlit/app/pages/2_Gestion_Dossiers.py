import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import datetime
import uuid

# --- NOUVEAUX IMPORTS REQUIS ---
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# -----------------------------------

# Importez vos modules de page (si vous modularisez plus tard)
# import ui_dossiers, ui_ingestion, ui_analyse, ui_confrontation

st.set_page_config(
    page_title="Dashboard Analyse", layout="wide", initial_sidebar_state="collapsed"
)


# --- 1. RÉCUPÉRATION DE LA SESSION SNOWFLAKE (via le cache) ---
@st.cache_resource
def create_snowpark_session():
    """
    Crée ou récupère la session Snowpark mise en cache.
    """
    try:
        connection_parameters = st.secrets["snowflake"]
        session = Session.builder.configs(connection_parameters).create()
        return session
    except Exception as e:
        st.error(f"Erreur de connexion à Snowflake : {e}")
        return None


# Appel pour récupérer la session
session = create_snowpark_session()

if not session:
    st.error("Connexion à Snowflake échouée. Impossible de charger l'application.")
    st.stop()
# -----------------------------------------------------------------


# --- 2. Vérification de l'Authentification ---
# (S'assurer que l'utilisateur est bien passé par 1_Authentification.py)
if not st.session_state.get("is_logged_in", False):
    st.error("Accès refusé. Veuillez vous authentifier.")
    # Assurez-vous que le chemin est correct (s'il est dans /pages)
    st.page_link("1_Authentification.py", label="Retour à l'authentification")
    st.stop()

# st.success(f"Connecté en tant que : {st.session_state.user_email}")

# --- 3. Barre de Navigation Supérieure (Horizontale) ---
selected_tab = option_menu(
    menu_title=None,
    options=[
        "Gestion Dossiers",
        "Ingestion Données",
        "Analyse Ligne/Boîtier",
        "Confrontation",
    ],
    icons=["folder2", "upload", "graph-up", "diagram-3"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

# --- 4. Gestion du Contenu de chaque Onglet ---

if selected_tab == "Gestion Dossiers":
    # --- Pré-requis ---
    # 'session' est maintenant disponible ici !
    # st.session_state['user_email'] est disponible

    # --- 1. Fonction de chargement des dossiers (avec cache) ---
    @st.cache_data(ttl=300)
    def load_user_dossiers(_session, user_email):
        """
        Interroge Snowflake pour récupérer les dossiers liés à l'utilisateur.
        """
        # st.write(f"Chargement des dossiers pour {user_email}...") # Optionnel
        try:
            query = """
                SELECT
                    DOSSIER_ID,
                    PV_NUMBER,
                    DIRECTEUR_ENQUETE,
                    DATE_SAISINE,
                FROM
                    DOSSIERS_DB.PROD.DOSSIERS
                WHERE
                    CREATED_BY_USER_EMAIL = ?
                ;
            """
            # Utilise la variable 'session' globale récupérée en haut
            dossiers_df = _session.sql(query, params=[user_email]).to_pandas()
            return dossiers_df
        except Exception as e:
            st.error(f"Erreur lors du chargement des dossiers : {e}")
            return pd.DataFrame()

    # --- 2. Interface de sélection de dossier ---
    st.header(f"Espace de travail de {st.session_state['user_email']}")
    st.markdown("---")

    # Chargement des données
    user_dossiers_df = load_user_dossiers(session, st.session_state["user_email"])

    if user_dossiers_df.empty:
        st.info("Vous n'avez aucun dossier. Veuillez en créer un.")
    else:
        st.subheader("📂 Sélectionner un dossier existant")
        dossier_options = {
            f"{row['PV_NUMBER']} ": row["DOSSIER_ID"]
            for index, row in user_dossiers_df.iterrows()
        }
        selected_dossier_name = st.selectbox(
            "Choisissez un dossier à analyser :", options=dossier_options.keys()
        )
        if selected_dossier_name:
            st.session_state["current_dossier_id"] = dossier_options[
                selected_dossier_name
            ]
            st.success(
                f"Dossier **{selected_dossier_name}** sélectionné. (ID: `{st.session_state['current_dossier_id']}`)"
            )
            with st.expander("Détails du dossier sélectionné"):
                dossier_details = user_dossiers_df[
                    user_dossiers_df["DOSSIER_ID"]
                    == st.session_state["current_dossier_id"]
                ]
                st.dataframe(dossier_details)

    st.markdown("---")

    # --- 3. Interface de création de dossier ---
    st.subheader("➕ Créer un nouveau dossier")
    with st.form("new_dossier_form"):
        st.markdown(
            """
        Veuillez renseigner les informations suivantes :`.
        """
        )
        pv_number = st.text_input(
            "N° de PV (Format: CODE_UNITE/NUMERO_PV/ANNEE)",
            placeholder="ex: 02489/0000/2025",
        )
        directeur_enquete = st.text_input("Nom du Directeur d'Enquête")
        date_saisine = st.date_input("Date de Saisine", datetime.date.today())
        submitted = st.form_submit_button("Créer le dossier")

    # --- 4. Logique de traitement du formulaire ---
    if submitted:
        if not pv_number or not directeur_enquete:
            st.error("Le N° de PV et le Nom du Directeur d'Enquête sont obligatoires.")
        else:
            try:
                with st.spinner("Création du dossier dans Snowflake..."):
                    new_dossier_id = str(uuid.uuid4())
                    current_time = datetime.datetime.now()
                    user_email = st.session_state["user_email"]
                    new_dossier_data = {
                        "DOSSIER_ID": [new_dossier_id],
                        "PV_NUMBER": [pv_number],
                        "DIRECTEUR_ENQUETE": [directeur_enquete],
                        "DATE_SAISINE": [date_saisine],
                        "CREATED_BY_USER_EMAIL": [user_email],
                    }
                    new_dossier_df = pd.DataFrame(new_dossier_data)

                    # Utilise la variable 'session' globale
                    session.write_pandas(
                        new_dossier_df,
                        table_name="DOSSIERS",
                        database="DOSSIERS_DB",
                        schema="PROD",
                        auto_create_table=False,
                        overwrite=False,
                    )
                st.success(f"Dossier **{pv_number}** créé avec succès !")
                st.balloons()
                load_user_dossiers.clear()
                st.session_state["current_dossier_id"] = new_dossier_id
                st.rerun()
            except SnowparkSQLException as e:
                st.error(f"Erreur SQL lors de la création du dossier : {e.message}")
            except Exception as e:
                st.error(f"Une erreur inattendue est survenue : {e}")

    if st.session_state.get("current_dossier_id"):
        # Ceci est optionnel, mais guide l'utilisateur
        st.info(
            f"Le dossier `{st.session_state['current_dossier_id']}` est actif. \n\n"
            "Passez à l'onglet 'Ingestion Données' pour charger vos fichiers."
        )

# --- Le reste de vos onglets (Ingestion, Analyse, ...) ---

elif selected_tab == "Ingestion Données":
    st.header("2. Ingestion des Données (MT20, MT24, HREF)")
    if not st.session_state.get("current_dossier_id"):
        st.warning(
            "Veuillez d'abord sélectionner ou créer un dossier dans l'onglet 'Gestion Dossiers'."
        )
    else:
        st.success(f"Dossier actif : {st.session_state['current_dossier_id']}")
        # Votre logique d'upload ici. Elle aura accès à la variable 'session'
        st.info(
            "Logique d'upload (RAW_DATA) et de déclenchement (Airflow) à implémenter ici."
        )

elif selected_tab == "Analyse Ligne/Boîtier":
    st.header("3. Analyse Ligne/Boîtier (MSISDN / IMEI)")
    if not st.session_state.get("current_dossier_id"):
        st.warning("Veuillez d'abord sélectionner un dossier.")
    else:
        # Votre logique d'analyse ici. Elle aura accès à la variable 'session'
        st.info("Logique d'analyse (lecture des MARTS) à implémenter ici.")

elif selected_tab == "Confrontation":
    st.header("4. Confrontation et Analyse de Zone")
    if not st.session_state.get("current_dossier_id"):
        st.warning("Veuillez d'abord sélectionner un dossier.")
    else:
        # Votre logique de confrontation ici. Elle aura accès à la variable 'session'
        st.info(
            "Logique d'analyse avancée (HREF, croisement de cibles) à implémenter ici."
        )
