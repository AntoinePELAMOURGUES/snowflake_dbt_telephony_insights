# 🏠Accueil.py (Votre fichier principal, exécutez avec 'streamlit run 🏠Accueil.py')

import streamlit as st
from st_pages import get_nav_from_toml, hide_pages

# --- Configuration Générale ---
# Doit être la première commande Streamlit
st.set_page_config(
    page_title="PROJET TELEPHONY INSIGHTS",
    page_icon="img/icone.png",  # Vérifiez le chemin vers votre icône
    layout="wide",
)

# Charge la structure de navigation depuis votre fichier TOML
# Par défaut, il cherche ".streamlit/pages.toml"
nav = get_nav_from_toml()

# Affiche le logo dans la barre latérale
st.logo("img/banniere.png")  # Vérifiez le chemin vers votre logo/bannière

# --- Gestion de l'Authentification ---
# C'est ici que la magie opère.
# En fonction de l'état de session, on affiche ou masque les pages.

if st.session_state.get("authenticated", False):
    # L'UTILISATEUR EST CONNECTÉ
    # On affiche toutes les pages privées
    hide_pages(["Accueil", "Authentification"])
else:
    # L'UTILISATEUR N'EST PAS CONNECTÉ
    # On masque toutes les pages privées définies dans le TOML
    hide_pages(
        [
            "Gestion des Dossiers",
            "Analyse Opérateurs",
            "Analyse Orange",
            "Analyse SRR",
            "Analyse TELCO",
            "Administration",
            "Page de Test",
        ]
    )

# --- Initialisation de la Navigation ---
pg = st.navigation(nav)

# Exécute la page sélectionnée par l'utilisateur (ou la première par défaut)
pg.run()
