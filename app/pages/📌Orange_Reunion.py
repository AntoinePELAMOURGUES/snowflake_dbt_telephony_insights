import streamlit as st
import pandas as pd
import requests
import base64
import logging
import os
from dotenv import load_dotenv
from modules.dashboard_streamlit import *
from modules.dashboard_streamlit_no_geoloc import *
from typing import Tuple

load_dotenv()

# Chargement des variables d'environnement
hostname_fastapi = os.getenv("HOST_FASTAPI")

######" GESTIONS DES LOGS "######

# Configuration du logger
logger = logging.getLogger("orre_logs")
logger.setLevel(logging.INFO)  # ou DEBUG selon le besoin

# Récupère le chemin absolu du dossier parent du fichier courant
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
logs_dir = os.path.join(parent_dir, "logs")

log_file_path = os.path.join(logs_dir, "orre_streamlit.log")
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")

# Ajouter le handler au logger (éviter les doublons)
if not logger.hasHandlers():
    logger.addHandler(file_handler)




################    FONCTIONS TEST COLONNES ADRESSE & TYPE D'APPEL     #################


def verifier_et_afficher_alertes(df: pd.DataFrame) -> None:
    """
    Vérifie les données techniques et affiche des alertes selon les normes MLOps.

    Parameters:
        df (pd.DataFrame): DataFrame contenant les données à analyser

    Returns:
        None: Affiche directement les éléments dans Streamlit
    """
    # 1. Vérification colonne ADRESSE
    adresse_ok, msg_adresse = _verifier_colonne_adresse(df)

    # 2. Vérification présence DATA dans correspondant
    data_ok, msg_data = _verifier_presence_data(df)

    # 3. Gestion des alertes
    _gerer_alertes(adresse_ok, msg_adresse, data_ok, msg_data)

    # 4. Affichage visualisation
    st.markdown("---")

    return adresse_ok

def _verifier_colonne_adresse(df: pd.DataFrame) -> Tuple[bool, str]:
    """Vérifie la présence et le contenu de la colonne ADRESSE"""
    colonnes_adresse = [col for col in df.columns if "ADRESSE" in col.upper()]

    if not colonnes_adresse:
        return False, "⚠️ **Colonne ADRESSE manquante** : Demander une nouvelle réquisition MT24"

    col = colonnes_adresse[0]
    if df[col].isna().all():
        return False, f"⚠️ **Colonne {col} vide** : Données de localisation manquantes"

    return True, ""

def _verifier_presence_data(df: pd.DataFrame) -> Tuple[bool, str]:
    """Vérifie la présence de DATA dans la colonne TYPE D'APPEL"""
    if "TYPE D'APPEL" not in df.columns:
        return False, "⚠️ **Colonne TYPE D'APPEL manquante** : Structure de données invalide"

    if "DATA" not in df["TYPE D'APPEL"].values:
        return False, "⚠️ **Valeur DATA absente** : Privilégier une réquisition IMEI MT24"

    return True, ""

def _gerer_alertes(adresse_ok: bool, msg_adresse: str,
                  data_ok: bool, msg_data: str) -> None:
    """Gère l'affichage des messages d'alerte cumulés"""
    messages = []
    if not adresse_ok:
        messages.append(msg_adresse)
    if not data_ok:
        messages.append(msg_data)

    if messages:
        st.warning("\n\n".join(messages))

    st.markdown("---")


######### GESTION DE LA PAGE #########

# Configuration de la page
st.set_page_config(page_title="Orange_Reunion", initial_sidebar_state="collapsed")


@st.cache_data
def get_img_as_base64(file):
    try:
        with open(file, "rb") as f:
            data = f.read()
        logger.info(f"Image chargée avec succès : {file} ({len(data)} octets)")
        return base64.b64encode(data).decode()
    except Exception as e:
        logger.error(f"Erreur lors du chargement de l'image {file}: {e}")
        return ""


img1 = get_img_as_base64("./img/background.png")
img2 = get_img_as_base64("./img/banniere.png")

page_bg_img = f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playwrite+IN:wght@100..400&display=swap');
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
        background-position: bottom left ;
        background-repeat: no-repeat;
        width: 100%;
    }}

    [data-testid="stDataFrame"] {{
        width: 100%;
    }}

    </style>
    """

st.markdown(
    page_bg_img,
    unsafe_allow_html=True,
)

# Réinitialiser la page si elle est chargée
if "page" not in st.session_state:
    st.session_state.page = "orange"
    logger.info("Session initialisée sur la page 'orange'.")

# Page Title
if st.session_state.page == "orange":
    st.markdown(
        """
    <div style='text-align: center; color: #d8a824; font-family: "Playwrite IN", monospace; font-size: 18px; padding-top: 100px;'>
        <h1>
            ORANGE REUNION
        </h1>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown(
        """
    <div style='text-align: center; color: #d8a824; font-family: "Playwrite IN", monospace; font-size: 18px; padding-top: 30px; padding-bottom: 30px;'>
        <h2>
            Veuillez choisir le type de réquisition que vous souhaitez analyser :
        </h2>
    </div>
""",
        unsafe_allow_html=True,
    )

    left, right = st.columns(2)
    if left.button("MT20 - Détail géolocalisé du n° d'appel", use_container_width=True):
        st.session_state.page = "mt24"
        logger.info("Changement de page : 'mt24' sélectionnée.")

    if right.button("MT24 - Détail géolocalisé du n° IMEI", use_container_width=True):
        st.session_state.page = "mt20"
        logger.info("Changement de page : 'mt20' sélectionnée.")

# Logic for MT24 & MT20 Page
if st.session_state.page == "mt24" or st.session_state.page == "mt20":

    st.write("Veuillez charger votre fichier csv :")

    uploaded_file_1 = st.file_uploader(
        "Fichier contenant les communications", type="csv"
    )

    if uploaded_file_1:
        file_name = uploaded_file_1.name
        file_size = len(uploaded_file_1.getvalue())
        logger.info(f"Fichier uploadé : {file_name} ({file_size} octets)")

        files = {
            "file1": (
                file_name,
                uploaded_file_1.getvalue(),
                "text/csv",
            )
        }
        try:
            response = requests.post(
                f"http://{hostname_fastapi}:8000/orre_mt20_mt24",
                files=files,
            )
            if response.ok:
                df = pd.DataFrame(response.json())
                adresse_ok = verifier_et_afficher_alertes(df)
                if adresse_ok:
                    dataviz(df, "ORRE")
                else:
                    no_loc_dataviz(df)
            else:
                logger.error(f"Erreur API : {response.status_code}")
                st.error("Erreur de récupération des données")
        except Exception as e:
            st.error(f"Erreur critique : {str(e)}")
