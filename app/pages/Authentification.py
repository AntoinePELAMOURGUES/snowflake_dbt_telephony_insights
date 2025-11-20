import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import bcrypt
import re

# Import du module de validation (Architecture modulaire)
from modules.utils import validate_password_strength


# ==============================================================================
# 1. COUCHE INFRASTRUCTURE & CONNEXION
# ==============================================================================
@st.cache_resource
def create_snowpark_session():
    """
    Établit une connexion Singleton à Snowflake.
    @st.cache_resource garantit que la session est créée une seule fois
    et partagée entre les rechargements du script pour la performance.
    """
    try:
        # Récupération sécurisée des secrets (jamais en dur dans le code)
        connection_parameters = st.secrets["snowflake"]
        session = Session.builder.configs(connection_parameters).create()
        return session
    except Exception as e:
        st.error(f"CRITICAL: Échec de connexion au Data Warehouse : {e}")
        return None


session = create_snowpark_session()

# Arrêt d'urgence si l'infrastructure ne répond pas
if not session:
    st.error("Service indisponible. Contactez l'administrateur.")
    st.stop()

# ==============================================================================
# 2. GESTION DE L'ÉTAT (SESSION STATE MANAGEMENT)
# ==============================================================================
# Initialisation des variables de session pour persister l'état entre les reruns
if "is_logged_in" not in st.session_state:
    st.session_state.is_logged_in = False
if "user_email" not in st.session_state:
    st.session_state.user_email = None

# Redirection automatique : UX pour éviter de voir le login si déjà connecté
if st.session_state.is_logged_in:
    st.success(f"Session active : **{st.session_state.user_email}**")
    st.page_link("pages/Gestion_Dossiers.py", label="Accéder au Dashboard", icon="📁")
    st.stop()  # Stoppe l'exécution ici pour ne pas charger le reste de la page login

# ==============================================================================
# 3. INTERFACE UTILISATEUR (FRONTEND)
# ==============================================================================
st.markdown(
    """
    <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px; padding-top: 10px;'>
        <h1>💥 Bienvenue dans Telephony-Insights 💥</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("---")
st.warning("Authentification requise pour accéder aux données sensibles.")

tabs = st.tabs(["🔒 Connexion", "📝 Inscription"])

# ==============================================================================
# 4. LOGIQUE DE CONNEXION (LOGIN)
# ==============================================================================
with tabs[0]:
    with st.form("connexion_form"):
        st.header("Connexion")
        email_connexion = st.text_input(
            "Email", placeholder="prenom.nom@gendarmerie.interieur.gouv.fr"
        )
        password_connexion = st.text_input("Mot de passe", type="password")
        submitted_connexion = st.form_submit_button("Se connecter", width="stretch")

        if submitted_connexion:
            if not email_connexion or not password_connexion:
                st.error("Identifiants manquants.")
            else:
                try:
                    # Data Cleaning : Indispensable pour matcher le format en base (minuscule)
                    email_login_clean = email_connexion.strip().lower()

                    # Requête sécurisée (Parameterized Query) contre Injection SQL
                    query = (
                        "SELECT PASSWORD_HASH FROM AUTH_DB.PROD.USERS WHERE EMAIL = ?;"
                    )
                    result = session.sql(query, params=[email_login_clean]).collect()

                    if result:
                        stored_hash = result[0]["PASSWORD_HASH"]
                        # Vérification cryptographique du mot de passe (Bcrypt gère le sel automatiquement)
                        if bcrypt.checkpw(
                            password_connexion.encode("utf-8"),
                            stored_hash.encode("utf-8"),
                        ):

                            # Mise à jour de la session
                            st.session_state.is_logged_in = True
                            st.session_state.user_email = email_login_clean
                            st.session_state.authenticated = True

                            st.success("Authentification réussie.")
                            st.switch_page("pages/Gestion_Dossiers.py")
                        else:
                            st.error("Échec : Mot de passe incorrect.")
                    else:
                        st.error("Échec : Utilisateur inconnu.")

                except Exception as e:
                    st.error(f"Erreur système lors de la connexion : {e}")

# ==============================================================================
# 5. LOGIQUE D'INSCRIPTION (SIGN UP)
# ==============================================================================
with tabs[1]:
    st.header("Créer un compte")

    # --- Pattern "Delayed Feedback" ---
    # Affiche le succès AVANT de redessiner le formulaire vide
    if st.session_state.get("success_inscription_msg"):
        st.success(st.session_state["success_inscription_msg"])
        st.balloons()
        st.session_state["success_inscription_msg"] = None  # Reset du message

    # --- Pattern "Form Reset Flag" ---
    # Vide les variables liées aux widgets AVANT leur instanciation
    if st.session_state.get("reset_inscription_form"):
        st.session_state["reg_user"] = ""
        st.session_state["reg_email"] = ""
        st.session_state["reg_service"] = ""
        st.session_state["reg_pass"] = ""
        st.session_state["reset_inscription_form"] = False

    # Formulaire avec clear_on_submit=False pour garder le contrôle manuel via Session State
    with st.form("form_inscription", clear_on_submit=False):
        # Utilisation de clés (key) pour le binding avec Session State
        user_inscription = st.text_input("Nom & Prénom", key="reg_user")
        email_inscription = st.text_input("Email Gendarmerie", key="reg_email")
        service_inscription = st.text_input("Unité / Service", key="reg_service")
        password_inscription = st.text_input(
            "Mot de passe", type="password", key="reg_pass"
        )
        submitted_inscription = st.form_submit_button("S'inscrire")

    if submitted_inscription:
        # Validation métier
        is_valid_pass, error_msg = validate_password_strength(password_inscription)

        if not email_inscription or not password_inscription or not user_inscription:
            st.error("Tous les champs sont obligatoires.")
        elif not email_inscription.endswith("@gendarmerie.interieur.gouv.fr"):
            st.error("Domaine de messagerie non autorisé.")
        elif not is_valid_pass:
            st.error(f"Sécurité mot de passe : {error_msg}")
        else:
            # --- Data Sanitization (Normalisation) ---
            user_clean = user_inscription.strip().upper()  # Standardisation MAJ
            service_clean = service_inscription.strip().upper()  # Standardisation MAJ
            email_clean = (
                email_inscription.strip().lower()
            )  # Standardisation min (Critique pour login)

            # --- Sécurisation (Hashing) ---
            try:
                hashed_password = bcrypt.hashpw(
                    password_inscription.encode("utf-8"), bcrypt.gensalt()
                ).decode("utf-8")
            except Exception as e:
                st.error(f"Erreur de chiffrement : {e}")
                st.stop()

            # --- Persistance (Snowflake) ---
            try:
                query = """
                    INSERT INTO AUTH_DB.PROD.USERS (NOM_PRENOM, EMAIL, SERVICE, PASSWORD_HASH)
                    VALUES (?, ?, ?, ?);
                """
                session.sql(
                    query,
                    params=[user_clean, email_clean, service_clean, hashed_password],
                ).collect()

                # Préparation du prochain affichage (Pattern Rerun)
                st.session_state["success_inscription_msg"] = (
                    f"Compte créé pour {user_clean}. Veuillez vous connecter."
                )
                st.session_state["reset_inscription_form"] = True

                # Force le rechargement pour appliquer le nettoyage et afficher le succès
                st.rerun()

            except SnowparkSQLException as e:
                if "PRIMARY KEY" in str(e) or "UNIQUE constraint" in str(e):
                    st.error("Cet email possède déjà un compte actif.")
                else:
                    st.error(f"Erreur Base de Données : {e}")
            except Exception as e:
                st.error(f"Erreur inattendue : {e}")
