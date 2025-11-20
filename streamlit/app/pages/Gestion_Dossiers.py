import streamlit as st
import pandas as pd
import uuid
from datetime import date
from snowflake.snowpark import Session


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


# Fonction de chargement des dossiers (Cachée pour perf, mais avec TTL pour rafraichir)
@st.cache_data(ttl=60)
def load_user_dossiers(user_email):
    try:
        query = """
            SELECT DOSSIER_ID, PV_NUMBER, NOM_DOSSIER, TYPE_ENQUETE, DATE_SAISINE, STATUS
            FROM DOSSIERS_DB.PROD.DOSSIERS
            WHERE CREATED_BY_USER_EMAIL = ?
            ORDER BY DATE_SAISINE DESC
        """
        return session.sql(query, params=[user_email]).to_pandas()
    except Exception as e:
        st.error(f"Erreur de chargement : {e}")
        return pd.DataFrame()


# --- UI START ---
st.title("📁 Gestion des Dossiers")

# Gestion du message de succès après rechargement (Pattern "Flash Message")
if st.session_state.get("dossier_created_msg"):
    st.success(st.session_state["dossier_created_msg"])
    st.balloons()
    st.session_state["dossier_created_msg"] = None  # On efface le message

# Les Onglets
tab_liste, tab_creation = st.tabs(["📂 Mes Dossiers", "➕ Nouveau Dossier"])

# ==============================================================================
# ONGLET 1 : LISTE ET SÉLECTION
# ==============================================================================
with tab_liste:
    st.header("Sélectionner un dossier de travail")

    user_email = st.session_state.get("user_email")
    if user_email:
        df_dossiers = load_user_dossiers(user_email)

        if not df_dossiers.empty:
            # Affichage Interactif
            # on_select="rerun" est CRUCIAL pour détecter le clic immédiatement
            event = st.dataframe(
                df_dossiers,
                column_config={
                    "DOSSIER_ID": None,  # On cache l'ID technique
                    "PV_NUMBER": "N° PV",
                    "NOM_DOSSIER": "Nom Dossier",
                    "TYPE_ENQUETE": "Type Enquête",
                    "DIRICTEUR_ENQUETE": "Directeur d'Enquête",
                    "DATE_SAISINE": st.column_config.DateColumn(
                        "Date Saisine", format="DD/MM/YYYY"
                    ),
                },
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",  # Une seule ligne sélectionnable
                on_select="rerun",
            )

            # --- LOGIQUE DE REDIRECTION À LA SÉLECTION ---
            if len(event.selection.rows) > 0:
                # 1. Récupération de l'index de la ligne sélectionnée
                selected_index = event.selection.rows[0]

                # 2. Récupération des données de cette ligne
                selected_dossier = df_dossiers.iloc[selected_index]
                dossier_id = selected_dossier["DOSSIER_ID"]
                dossier_nom = selected_dossier["NOM_DOSSIER"]

                # 3. Mise en session
                st.session_state.current_dossier_id = dossier_id
                st.session_state.current_dossier_name = dossier_nom

                # 4. Feedback visuel rapide
                st.success(f"Dossier '{dossier_nom}' chargé. Redirection...")

                # 5. REDIRECTION VERS MES DONNÉES
                st.switch_page("pages/Mes_Donnees.py")

        else:
            st.info(
                "Aucun dossier trouvé. Créez-en un dans l'onglet 'Nouveau Dossier'."
            )
    else:
        st.error("Session expirée. Veuillez vous reconnecter.")

# ==============================================================================
# ONGLET 2 : CRÉATION
# ==============================================================================
with tab_creation:
    st.header("Initialiser une nouvelle affaire")

    with st.form(
        "form_creation_dossier", clear_on_submit=True
    ):  # True ici pour vider après succès
        col1, col2 = st.columns(2)
        with col1:
            pv_number = st.text_input("Numéro de PV *", placeholder="Ex: 12345/2025")
            nom_dossier = st.text_input(
                "Nom de l'opération *", placeholder="Ex: OP STUPS"
            )
            type_enquete = st.selectbox(
                "Cadre *", ["", "Flagrance", "Préliminaire", "Commission Rogatoire"]
            )
        with col2:
            directeur_enquete = st.text_input("Directeur d'Enquête *")
            date_saisine = st.date_input("Date de saisine *", value=date.today())

        submit = st.form_submit_button("Créer le dossier", type="primary")

    if submit:
        # Validation
        required_fields = {
            "Numéro de PV": pv_number,
            "Nom du dossier": nom_dossier,
            "Cadre d'enquête": type_enquete,
            "Directeur d'Enquête": directeur_enquete,
        }
        missing = [k for k, v in required_fields.items() if not v]

        if missing:
            st.error(f"⛔ Champs manquants : {', '.join(missing)}")
        else:
            try:
                # Préparation des données
                new_id = str(uuid.uuid4())
                email = st.session_state.user_email

                df_new = pd.DataFrame(
                    [
                        {
                            "DOSSIER_ID": new_id,
                            "PV_NUMBER": pv_number,
                            "NOM_DOSSIER": nom_dossier.strip().upper(),
                            "TYPE_ENQUETE": type_enquete.strip().upper(),
                            "DIRECTEUR_ENQUETE": directeur_enquete.strip().upper(),
                            "DATE_SAISINE": date_saisine,
                            "CREATED_BY_USER_EMAIL": email,
                        }
                    ]
                )

                # Insertion Snowflake
                session.write_pandas(
                    df_new,
                    "DOSSIERS",
                    database="DOSSIERS_DB",
                    schema="PROD",
                    auto_create_table=False,
                    overwrite=False,
                )

                # --- SUCCÈS : ON RESTE SUR LA PAGE ---

                # 1. On vide le cache pour forcer le rechargement de la liste dans l'onglet 1
                load_user_dossiers.clear()

                # 2. On prépare un message pour le rechargement
                st.session_state["dossier_created_msg"] = (
                    f"✅ Dossier {pv_number} créé ! Retrouvez-le dans l'onglet 'Mes Dossiers'."
                )

                # 3. On recharge la page (st.rerun ramène souvent au premier onglet par défaut, ce qui est parfait ici)
                st.rerun()

            except Exception as e:
                st.error(f"Erreur technique : {e}")
