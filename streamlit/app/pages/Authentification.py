import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
import bcrypt
import re


# --- 1. CONNEXION SNOWFLAKE ---
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
    st.error("Connexion à Snowflake échouée. Vérifiez la configuration.")
    st.stop()

# --- 2. GESTION DE L'ÉTAT DE SESSION ---
# Initialisation simplifiée
if "is_logged_in" not in st.session_state:
    st.session_state.is_logged_in = False
if "user_email" not in st.session_state:
    st.session_state.user_email = None

# Si déjà connecté, on n'affiche pas la page
if st.session_state.is_logged_in:
    st.success(
        f"Vous êtes déjà connecté en tant que : **{st.session_state.user_email}**"
    )
    st.page_link(
        "pages/2_Gestion_Dossiers.py", label="Accéder à l'application", icon="📁"
    )
    st.stop()

# --- 3. INTERFACE (Votre UI) ---
st.markdown(
    """
    <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", "Playwrite IN", monospace; font-size: 18px; padding-top: 10px;'>
        <h1>
            💥 Bienvenue dans Telephony-Insights 💥
        </h1>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

st.warning("Veuillez vous inscrire ou vous connecter pour accéder à votre Dashboard.")

tabs = st.tabs(["🔒 Connexion", "📝 Inscription"])

# --- 4. LOGIQUE DE CONNEXION (Simplifiée) ---
with tabs[0]:
    with st.form("connexion_form"):
        st.header("Connexion")
        email_connexion = st.text_input(
            "Email de connexion", placeholder="prenom.nom@gendarmerie.interieur.gouv.fr"
        )
        password_connexion = st.text_input("Mot de passe", type="password")
        submitted_connexion = st.form_submit_button(
            "Se connecter", use_container_width=True
        )

        if submitted_connexion:
            if not email_connexion or not password_connexion:
                st.error("Veuillez saisir l'email et le mot de passe.")
            else:
                try:
                    # Cible : AUTH_DB.PROD.USERS
                    query = "SELECT PASSWORD_HASH FROM USERS WHERE EMAIL = ?;"

                    # Exécution Snowpark
                    result = session.sql(query, params=[email_connexion]).collect()

                    if result:
                        stored_hash = result[0]["PASSWORD_HASH"]

                        # Vérification du hash
                        if bcrypt.checkpw(
                            password_connexion.encode("utf-8"),
                            stored_hash.encode("utf-8"),
                        ):
                            st.success("Connexion réussie !")
                            st.balloons()

                            # Stockage en session
                            st.session_state.is_logged_in = True
                            st.session_state.user_email = email_connexion
                            st.session_state.authenticated = True

                            # Redirection
                            st.switch_page("Gestion_Dossiers.py")
                        else:
                            st.error("Mot de passe incorrect.")
                    else:
                        st.error("Utilisateur non trouvé.")

                except Exception as e:
                    st.error(f"Erreur lors de la connexion : {e}")

# --- 5. LOGIQUE D'INSCRIPTION (Simplifiée) ---
with tabs[1]:
    with st.form("registration_form", clear_on_submit=True):
        st.header("Inscription")

        st.markdown(
            """
            - L'email doit se terminer par `@gendarmerie.interieur.gouv.fr`.
            - Le mot de passe doit être sécurisé.
            """
        )
        email_inscription = st.text_input(
            "Email Gendarmerie", placeholder="prenom.nom@gendarmerie.interieur.gouv.fr"
        )
        password_inscription = st.text_input("Mot de passe", type="password")

        submitted_inscription = st.form_submit_button(
            "S'inscrire", use_container_width=True
        )

        if submitted_inscription:
            # --- Validation des entrées ---
            if not email_inscription or not password_inscription:
                st.error("Tous les champs sont obligatoires.")
            elif not email_inscription.endswith("@gendarmerie.interieur.gouv.fr"):
                st.error("L'email doit être une adresse Gendarmerie valide.")
            # (Ici, vous devriez ajouter une vraie validation de force de mot de passe)
            elif len(password_inscription) < 12:
                st.error("Le mot de passe doit faire au moins 12 caractères.")
            else:
                # --- Hachage du mot de passe (JAMAIS en clair) ---
                try:
                    hashed_password = bcrypt.hashpw(
                        password_inscription.encode("utf-8"), bcrypt.gensalt()
                    ).decode("utf-8")
                except Exception as e:
                    st.error(f"Erreur lors de la sécurisation du mot de passe: {e}")
                    st.stop()

                # --- Insertion dans Snowflake ---
                try:
                    query = """
                        INSERT INTO AUTH_DB.PROD.USERS (EMAIL, PASSWORD_HASH)
                        VALUES (?, ?);
                    """

                    # Exécution Snowpark
                    session.sql(
                        query, params=[email_inscription, hashed_password]
                    ).collect()

                    st.success(
                        f"Utilisateur '{email_inscription}' créé avec succès ! Vous pouvez maintenant vous connecter."
                    )
                    st.balloons()

                except SnowparkSQLException as e:
                    # Gestion des erreurs courantes (ex: doublons)
                    if "PRIMARY KEY" in str(e) or "UNIQUE constraint" in str(e):
                        st.error("Erreur : Cet email est déjà utilisé.")
                    else:
                        st.error(f"Erreur lors de l'enregistrement : {e}")
                except Exception as e:
                    st.error(f"Une erreur inattendue est survenue : {e}")
