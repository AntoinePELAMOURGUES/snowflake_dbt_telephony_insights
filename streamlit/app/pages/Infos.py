import streamlit as st
import base64

# Configuration de la page
st.set_page_config(page_title="Infos", initial_sidebar_state="collapsed")


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
        max-width: 100vw;
        width: 100vw;
        padding-left: 10rem;
        padding-right: 10rem;
        padding-top: 2rem;
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

st.warning(
    """
**⚖ INFORMATIONS LÉGALES ⚖**

- **Cadre légal :** Conformément aux articles 230-20 à 230-27 et R40-39 à R40-41 du Code de procédure pénale français, l’utilisation de cette application ne nécessite pas l’autorisation d’un magistrat. Il ne s’agit pas d’un logiciel de confrontation de données, mais uniquement d’un outil d’analyse ponctuelle de fichiers FADET, sans base de données ni sauvegarde.
- **Respect du RGPD :** Aucune donnée n’est stockée ou conservée après analyse. L’application respecte les principes de minimisation, d’absence de profilage et de non-conservation des données.
- **Utilisation en procédure :** Il n’est pas nécessaire de mentionner l’utilisation de ce logiciel dans la procédure : il suffit de copier les éléments pertinents dans le dossier.
- **Complémentarité :** Ce logiciel n’a pas vocation à remplacer Devery Analytics, qui reste nécessaire pour les confrontations de données dans le cadre judiciaire.
    """
)

st.info(
    """
**📱 BONNES PRATIQUES EN TELEPHONIE**

- **🚨 Privilégier les MT24 - Détail géolocalisé du trafic à partir d'un IMEI :**
  Le suivi d'un suspect changeant régulièrement de SIM sera facilité.
  De plus, pour des raisons techniques liées aux opérateurs virtuels souvent absents des MT20, vous aurez un nombre bien plus important de données permettant d'améliorer considérablement la localisation.

- **⚠ Attention aux IMEI génériques :**
  Un IMEI est censé être un identifiant unique à 15 chiffres pour chaque appareil mobile. Cependant, dans la pratique judiciaire et l'analyse de FADETS, il arrive fréquemment de rencontrer des IMEI dits "génériques" ou "falsifiés", comme **000000000000000** ou **123456789012345** ou des **séquences répétitives**. Ces IMEI génériques peuvent apparaître pour plusieurs raisons :
    - Certains téléphones modifiés ou "rootés" peuvent perdre leur IMEI d'origine et afficher un IMEI par défaut ou générique
    - Plusieurs appareils (souvent de la contrefaçon ou des téléphones volés) partagent le même IMEI, ce qui rend impossible de relier un événement à un appareil précis
    - Sur certains modèles, notamment les appareils bas de gamme ou reconditionnés, l'IMEI peut être mal programmé ou absent.

  ‼ En conséquence, si vous basez votre analyse sur un IMEI générique, vous risquez d'attribuer à tort des communications ou des déplacements à un suspect, alors qu'ils peuvent concerner plusieurs appareils ou utilisateurs différents. Cela génère des faux positifs et fausse l'enquête.

  **🎯 Comment le détecter :**
  Utiliser une réquisition MT14 permettant d'obtenir, pour un IMEI donné, la liste de toutes les cartes SIM (IMSI/MSISDN) qui ont été insérées dans ce téléphone sur une période donnée. Elle doit être faite de préférence à tous les opérateurs. Cela permet de détecter si un IMEI suspect (générique ou non) est partagé entre plusieurs utilisateurs ou si un appareil est utilisé avec de multiples SIM, ce qui est typique des pratiques frauduleuses ou des tentatives de dissimulation.
    """
)


st.markdown("---")

# Title and logo
st.markdown(
    """
    <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC","Playwrite IN", monospace; font-size: 18px; padding-top: 10px;'>
        <h1>
            ☎️ Choisissez l'opérateur
        </h1>
    </div>
""",
    unsafe_allow_html=True,
)

st.markdown("---")

# Boutons pour choisir l'opérateur
left, middle, right = st.columns(3)
if left.button("ORANGE REUNION", use_container_width=True):
    st.session_state.operator = "orange"
    st.switch_page("pages/📌Orange_Reunion.py")

if middle.button("SRR", use_container_width=True):
    st.session_state.operator = "sfr"
    st.switch_page("pages/📌SRR.py")

if right.button("TELCO", use_container_width=True):
    st.session_state.operator = "telco"
    st.switch_page("pages/📌TELCO.py")

st.markdown("---")
