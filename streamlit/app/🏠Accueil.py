import streamlit as st
import base64
import os
import logging
import streamlit as st

# Configuration de l'application Streamlit

st.set_page_config(
    page_title="Telephony DataViz App",
    page_icon="./img/icone.png",
    initial_sidebar_state="collapsed",
)

######" GESTIONS DES LOGS "######

# Création du dossier logs s'il n'existe pas
os.makedirs("logs", exist_ok=True)

# Configuration du logger
logger = logging.getLogger("streamlit_app")
logger.setLevel(logging.INFO)  # ou DEBUG selon le besoin

# FileHandler pour écrire dans logs/streamlit_app.log
file_handler = logging.FileHandler("logs/streamlit_app.log", encoding="utf-8")
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s"
)
file_handler.setFormatter(formatter)

# Ajouter le handler au logger (éviter les doublons)
if not logger.hasHandlers():
    logger.addHandler(file_handler)

logger.info("Démarrage de l'application Streamlit")


@st.cache_data
def get_img_as_base64(file):
    with open(file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()


img1 = get_img_as_base64("./img/background.png")
img2 = get_img_as_base64("./img/banniere.png")
img3 = get_img_as_base64("./img/logo.png")


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
    }}

    [data-testid="stMainBlockContainer"] {{
        max-width: 100vw;
        width: 100vw;
        padding-left: 10rem;
        padding-right: 10rem;
    }}

    [data-testid="stHeader"] {{
        background: rgba(0, 0, 0, 0);
    }}

    [data-testid="stToolbar"] {{
        right: 2rem;
    }}

    [data-testid="stSidebar"] {{
        background-image: url("data:image/png;base64,{img2}");
        background-position: bottom;
    }}
    </style>
    """

st.markdown(
    page_bg_img,
    unsafe_allow_html=True,
)

st.markdown("---")

st.markdown(
    f"""
    <div style='display: flex; justify-content: center; align-items: center;'>
        <img src="data:image/png;base64,{img3}" width="800"
        style="border: 2px solid #fff; border-radius: 20px; box-shadow: 0 4px 24px rgba(0,0,0,0.1);" />
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playwrite+IN:wght@100..400&display=swap');
    @import url("https://fonts.googleapis.com/css2?family=IM+Fell+French+Canon+SC&display=swap");

    div.stButton {  <!-- Conteneur parent du bouton -->
        width: 100%;
        display: flex;
        justify-content: center;
    }

    div.stButton > button {
        width: 20%;
        height: 0.5em;
        font-size: 4em;
        font-family: 'Playwrite IN', sans-serif;
        font-weight: 600;
        background-color: #111;
        color: #fff;
        border: 1px solid #fff;
        border-radius: 1.5em;
        transition: background 0.3s, color 0.3s;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15);
        letter-spacing: 0.05em;
        margin: 0 auto;  <!-- Redondance pour compatibilité -->
    }
    div.stButton > button:hover {
        background: #222;
        color: #d8a824;
        border-color: #d8a824;
        cursor: pointer;
    }
    </style>
""",
    unsafe_allow_html=True,
)

# Bouton centré
start_button = st.button("Démarrer l'application")
if start_button:
    st.switch_page("pages/🚨Infos.py")
