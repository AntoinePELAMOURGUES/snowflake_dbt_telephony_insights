import streamlit as st
from streamlit_option_menu import option_menu

# Importez vos modules de page
# (il est bon de séparer la logique de chaque onglet dans des fichiers .py)
# import ui_dossiers, ui_ingestion, ui_analyse, ui_confrontation

st.set_page_config(
    page_title="Dashboard Analyse", layout="wide", initial_sidebar_state="collapsed"
)


@st.cache_data
def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


img1 = get_img_as_base64("./img/background.png")
img2 = get_img_as_base64("./img/banniere.png")


page_bg_img = f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playwrite+IN:wght@100..400&display=swap');
    @import url("https://fonts.googleapis.com/css2?family=IM+Fell+French+Canon+SC&display=swap");


    [data-testid="stAppViewContainer"] {{
        background-image: url("data:image/png;base64,{img1}");
        background-position: center;
        background-repeat: no-repeat;
        background-size: cover;
        max-width: 100vw;
        padding : 0;
        font-family: "IM Fell French Canon SC", "Playwrite IN", monospace;
        font-size: 18px;

    }}

    [data-testid="stMainBlockContainer"] {{
        padding-left: 0rem !important;
        padding-right: 0rem !important;
        padding-top: 0rem !important;
    }}


    [data-testid="stHeader"] {{
        background: rgba(0, 0, 0, 0);
    }}

    [data-testid="stToolbar"] {{
        right: 2rem;
    }}

    [data-testid="stSidebar"] {{
        background-image: url("data:image/png;base64,{img2}");
        background-position: bottom left ;
        background-repeat: no-repeat;
        width: 100%;
    }}
    </style>
    """

st.markdown(
    page_bg_img,
    unsafe_allow_html=True,
)


# --- 1. Vérification de l'Authentification ---
# (S'assurer que l'utilisateur est bien passé par 1_Authentification.py)
if not st.session_state.get("is_logged_in", False):
    st.error("Accès refusé. Veuillez vous authentifier.")
    st.page_link("pages/1_Authentification.py", label="Retour à l'authentification")
    st.stop()

# st.success(f"Connecté en tant que : {st.session_state.user_email}")

# --- 2. Barre de Navigation Supérieure (Horizontale) ---
selected_tab = option_menu(
    menu_title=None,  # Requis, mais peut être None
    options=[
        "Gestion Dossiers",
        "Ingestion Données",
        "Analyse Ligne/Boîtier",
        "Confrontation",
    ],
    icons=["folder2-open", "upload", "phone-find", "diagram-3"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {
            "padding": "0.5rem 1.5rem 0!important",
            "background-color": "#271E4D",
        },
        "icon": {"color": "#E3E64C", "font-size": "18px"},
        "nav-link": {
            "font-size": "16px",
            "text-align": "center",
            "margin": "0px",
            "--hover-color": "#eee",
        },
        "nav-link-selected": {
            "background-color": "#9b9ce5",
            "color": "#E3E64C",
            "font-weight": "700",
        },  # Couleur Gendarmerie/validation
    },
)

# --- 3. Gestion du Contenu de chaque Onglet ---

if selected_tab == "Gestion Dossiers":
    st.header("1. Gestion des Dossiers (Créer ou Sélectionner)")
    # --- Logique de cet onglet ---
    # 1. Afficher Selectbox des dossiers existants (SELECT ... FROM DOSSIERS_DB)
    # 2. Afficher Formulaire de création (INSERT ... INTO DOSSIERS_DB)
    # 3. Stocker le choix dans st.session_state['current_dossier_id']
    st.info("Logique de sélection/création de dossier (DOSSIERS_DB) à implémenter ici.")

elif selected_tab == "Ingestion Données":
    st.header("2. Ingestion des Données (MT20, MT24, HREF)")
    # --- Logique de cet onglet ---
    if not st.session_state.get("current_dossier_id"):
        st.warning(
            "Veuillez d'abord sélectionner ou créer un dossier dans l'onglet 'Gestion Dossiers'."
        )
    else:
        st.success(f"Dossier actif : {st.session_state['current_dossier_id']}")
        # 1. st.file_uploader (multi-files)
        # 2. Logique de parsing Pandas + ajout DOSSIER_ID
        # 3. session.write_pandas(...) vers tables RAW
        # 4. Bouton "Lancer transformation" (Appel API Airflow)
        st.info(
            "Logique d'upload (RAW_DATA) et de déclenchement (Airflow) à implémenter ici."
        )

elif selected_tab == "Analyse Ligne/Boîtier":
    st.header("3. Analyse Ligne/Boîtier (MSISDN / IMEI)")
    # --- Logique de cet onglet ---
    if not st.session_state.get("current_dossier_id"):
        st.warning("Veuillez d'abord sélectionner un dossier.")
    else:
        # 1. Input pour MSISDN/IMEI
        # 2. Requêtes SQL sur MARTS.FCT_COMMUNICATIONS et MARTS.DIM_DEVICE_LINKS
        #    (TOUJOURS filtré par st.session_state['current_dossier_id'])
        # 3. Affichage : st.map, st.bar_chart, st.dataframe (liaisons)
        st.info("Logique d'analyse (lecture des MARTS) à implémenter ici.")

elif selected_tab == "Confrontation":
    st.header("4. Confrontation et Analyse de Zone")
    # --- Logique de cet onglet ---
    if not st.session_state.get("current_dossier_id"):
        st.warning("Veuillez d'abord sélectionner un dossier.")
    else:
        st.info(
            "Logique d'analyse avancée (HREF, croisement de cibles) à implémenter ici."
        )
