import streamlit as st
from st_clickable_images import clickable_images

# Configuration de la page
st.set_page_config(page_title="Menu")

# Titre de la page
# Title and logo
st.markdown("""
    <div style='text-align: center; color: #d8a824; font-family: "Playwrite IN", monospace; font-size: 18px;'>
        <h1>
            ☎️ Choisissez l'opérateur
        </h1>
    </div>
""", unsafe_allow_html=True)



# Boutons pour choisir l'opérateur
left, middle, right = st.columns(3)
if left.button("ORANGE REUNION", use_container_width=True):
    st.session_state.operator = "orange"
    st.switch_page("pages/orange.py")

if middle.button("SRR", use_container_width=True):
    st.session_state.operator = "sfr"
    st.switch_page("pages/srr.py")

if right.button("TELCO", use_container_width=True):
    st.session_state.operator = "telco"
    st.switch_page("pages/telco.py")