import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import datetime
import uuid
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException


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

# --- 3. HEADER ET FORMATAGE DU NOM ---
user_email = st.session_state.get("user_email", "utilisateur.inconnu@...")

try:
    local_part = user_email.split("@")[0]
    parts = local_part.split(".")
    if len(parts) >= 2:
        prenom = parts[0]
        nom = " ".join(parts[1:])
        display_name = f"{prenom.title()} {nom.upper()}"
    else:
        display_name = local_part.title()
except Exception:
    display_name = user_email

# ==============================================================================
#  ZONE DE DÉFINITION DES FONCTIONS (GLOBALES)
# ==============================================================================


@st.cache_data(ttl=300)
def load_user_dossiers(_session, user_email):
    try:
        query = """
            SELECT
                DOSSIER_ID,
                PV_NUMBER,
                NOM_DOSSIER,
                TYPE_ENQUETE,
                DIRECTEUR_ENQUETE,
                DATE_SAISINE
            FROM
                DOSSIERS_DB.PROD.DOSSIERS
            WHERE
                CREATED_BY_USER_EMAIL = ?
            ORDER BY CREATED_AT DESC;
        """
        dossiers_df = _session.sql(query, params=[user_email]).to_pandas()
        return dossiers_df
    except Exception as e:
        st.error(f"Erreur lors du chargement des dossiers : {e}")
        return pd.DataFrame()


# ==============================================================================
#  FIN DES FONCTIONS GLOBALLES
# ==============================================================================

st.markdown(
    f"""
    <h1 style='text-align: center; color: #0055A4; font-size: 40px;'>
        Espace de travail de <span style='color: #0055A4'>{display_name}</span>
    </h1>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")

# --- 4. MENU DE NAVIGATION ---
selected_tab = option_menu(
    menu_title=None,
    options=[
        "Mes Dossiers en cours",
        "Créer un nouveau Dossier",
    ],
    icons=["folder-check", "folder-plus"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

# --- 5. CONTENU DES ONGLETS ---

if selected_tab == "Mes Dossiers en cours":

    # Chargement
    user_dossiers_df = load_user_dossiers(session, st.session_state["user_email"])

    if user_dossiers_df.empty:
        st.info(
            "Vous n'avez aucun dossier en cours. Allez dans l'onglet 'Créer un nouveau Dossier'."
        )
    else:
        # Affichage du tableau propre
        st.dataframe(
            user_dossiers_df,
            column_config={
                "DOSSIER_ID": None,  # Caché
                "PV_NUMBER": "N° PV",
                "NOM_DOSSIER": "Nom du Dossier",
                "TYPE_ENQUETE": "Type",
                "DIRECTEUR_ENQUETE": "Directeur d'Enquête",
                "DATE_SAISINE": "Date Saisine",
            },
            hide_index=True,
            use_container_width=True,
        )

        st.markdown("---")
        st.subheader("📂 Ouvrir un dossier")

        # Création de la liste de sélection
        # Gestion du cas où NOM_DOSSIER serait vide (pour les anciens dossiers)
        dossier_options = {}
        for index, row in user_dossiers_df.iterrows():
            label_nom = row["NOM_DOSSIER"] if row["NOM_DOSSIER"] else "Sans nom"
            label = f"{row['PV_NUMBER']} - {label_nom}"
            dossier_options[label] = row["DOSSIER_ID"]

        selected_dossier_label = st.selectbox(
            "Choisissez un dossier à analyser :", options=dossier_options.keys()
        )

        # Bouton d'action pour valider le choix (plus ergonomique qu'un switch immédiat)
        if st.button("Accéder au Dossier >>", type="primary"):
            if selected_dossier_label:
                st.session_state["current_dossier_id"] = dossier_options[
                    selected_dossier_label
                ]
                st.success(f"Chargement du dossier {selected_dossier_label}...")

                # CORRECTION NAVIGATION : Assurez-vous que le fichier existe
                # Note : st.switch_page attend un chemin relatif au dossier racine ou dans /pages
                try:
                    st.switch_page(
                        "pages/Mes_Donnees.py"
                    )  # Adaptez le nom du fichier si nécessaire
                except Exception as e:
                    st.error(
                        f"Page introuvable (Vérifiez le nom du fichier 'Mes_Donnees.py') : {e}"
                    )

elif selected_tab == "Créer un nouveau Dossier":

    st.subheader("➕ Initialiser une nouvelle affaire")
    with st.form("new_dossier_form"):
        col1, col2 = st.columns(2)

        with col1:
            pv_number = st.text_input(
                "N° de PV *",
                placeholder="ex: 02489/0000/2025",
                help="Format recommandé : CODE_UNITE/NUMERO/ANNEE",
            )
            nom_dossier = st.text_input(
                "Nom du Dossier (Opération)", placeholder="ex: OP STUPS 38"
            )

        with col2:
            directeur_enquete = st.text_input("Nom du Directeur d'Enquête *")
            type_enquete = st.selectbox(
                "Type d'Enquête",
                options=[
                    "Enquête Préliminaire",
                    "Enquête de Flagrance",
                    "Commission Rogatoire",
                    "Renseignement Administratif",
                    "Autre",
                ],
            )

        date_saisine = st.date_input("Date de Saisine", datetime.date.today())

        st.markdown("*Champs obligatoires")
        submitted = st.form_submit_button("Créer le dossier", use_container_width=True)

    if submitted:
        if not pv_number or not directeur_enquete:
            st.error("Le N° de PV et le Nom du Directeur d'Enquête sont obligatoires.")
        else:
            try:
                with st.spinner("Création du dossier dans Snowflake..."):
                    new_dossier_id = str(uuid.uuid4())
                    # On n'envoie PAS le CREATED_AT depuis Python pour éviter l'erreur de timestamp
                    # Snowflake utilisera son DEFAULT CURRENT_TIMESTAMP()
                    user_email = st.session_state["user_email"]

                    new_dossier_data = {
                        "DOSSIER_ID": [new_dossier_id],
                        "PV_NUMBER": [pv_number],
                        "NOM_DOSSIER": [nom_dossier],
                        "TYPE_ENQUETE": [type_enquete],
                        "DIRECTEUR_ENQUETE": [directeur_enquete],
                        "DATE_SAISINE": [date_saisine],
                        "CREATED_BY_USER_EMAIL": [user_email],
                    }
                    new_dossier_df = pd.DataFrame(new_dossier_data)

                    session.write_pandas(
                        new_dossier_df,
                        table_name="DOSSIERS",
                        database="DOSSIERS_DB",
                        schema="PROD",
                        auto_create_table=False,
                        overwrite=False,
                    )

                st.success(f"Dossier {pv_number} créé avec succès !")
                st.balloons()

                # Mise à jour du contexte
                load_user_dossiers.clear()
                st.session_state["current_dossier_id"] = new_dossier_id

                # Redirection immédiate vers l'ingestion
                try:
                    st.switch_page("pages/Mes_Donnees.py")
                except:
                    st.warning(
                        "Dossier créé, mais impossible de rediriger vers Mes_Donnees (fichier introuvable)."
                    )

            except SnowparkSQLException as e:
                st.error(f"Erreur SQL : {e.message}")
            except Exception as e:
                st.error(f"Une erreur inattendue est survenue : {e}")
