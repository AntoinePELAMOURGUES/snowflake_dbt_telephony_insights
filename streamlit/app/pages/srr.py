import streamlit as st
from src.srr_preprocess_mt20_mt24 import preprocess_data
from src.streamlit_dataviz import visualisation_data
import pandas as pd

# Réinitialiser la page si elle est chargée
if 'page' not in st.session_state:
    st.session_state.page = "srr"

# Page Title
if st.session_state.page == "srr":
    st.markdown("""
    <div style='text-align: center; color: #d8a824; font-family: "Playwrite IN", monospace; font-size: 18px;'>
        <h1>
            SRR
        </h1>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('---')

    st.write("Veuillez choisir le type de réquisition que vous souhaitez analyser :")

    left, right = st.columns(2)
    if left.button("MT20", use_container_width=True):
        st.session_state.page = "mt24"

    if right.button("MT24", use_container_width=True):
        st.session_state.page = "mt20"

# Logic for MT24 Page
if st.session_state.page == "mt24" or st.session_state.page == "mt20":
    st.write("Veuillez charger vos 2 fichiers xls :")

    uploaded_file_1 = st.file_uploader("Fichier contenant les communications ('SRR_Detcom...')", type="xls")
    uploaded_file_2 = st.file_uploader("Fichier contenant les localisations de relais ('SRR_Ident...)", type="xls")

    if uploaded_file_1 and uploaded_file_2:
        fichier_excel = pd.ExcelFile(uploaded_file_1)
        noms_feuilles = fichier_excel.sheet_names
        if len(noms_feuilles) > 1:
            st.warning("Le fichier contient plusieurs feuilles. Veuillez sélectionner la feuille contenant les données que vous souhaitez analyser:")
            feuille = st.selectbox("Feuilles disponibles", noms_feuilles)
            df = preprocess_data(uploaded_file_1, uploaded_file_2, sheet_name=feuille)
        else:
            df = preprocess_data(uploaded_file_1, uploaded_file_2, sheet_name=0)
        st.markdown("---")
        visualisation_data(df, 'SRR')