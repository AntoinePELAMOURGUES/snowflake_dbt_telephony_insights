import streamlit as st
from src.orre_preprocess_mt20_mt24 import preprocess_data
from src.streamlit_dataviz import visualisation_data

# Réinitialiser la page si elle est chargée
if 'page' not in st.session_state:
    st.session_state.page = "orange"

# Page Title
if st.session_state.page == "orange":
    st.markdown("""
    <div style='text-align: center; color: #d8a824; font-family: "Playwrite IN", monospace; font-size: 18px;'>
        <h1>
            ORANGE REUNION
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

    st.write("Veuillez charger votre fichier csv :")

    uploaded_file_1 = st.file_uploader("Fichier contenant les communications", type="csv")

    if uploaded_file_1:
        df = preprocess_data(uploaded_file_1)
        st.markdown("---")
        visualisation_data(df, 'ORRE')