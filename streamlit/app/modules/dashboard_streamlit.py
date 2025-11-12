import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from .dataviz_functions import *
import logging
import os

###########      VISUALISATION SANS NOTION DE LOCALISATION          #########


######" GESTIONS DES LOGS "######

# Configuration du logger
logger = logging.getLogger("dataviz_logs")
logger.setLevel(logging.INFO)  # ou DEBUG selon le besoin

# Récupère le chemin absolu du dossier parent du fichier courant
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
logs_dir = os.path.join(parent_dir, "logs")

# Crée le dossier logs s'il n'existe pas
os.makedirs(logs_dir, exist_ok=True)

log_file_path = os.path.join(logs_dir, "telephony_dataviz_streamlit.log")
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")

# Ajouter le handler au logger (éviter les doublons)
if not logger.hasHandlers():
    logger.addHandler(file_handler)


################################################### FONCTION AVEC NOTION DE VISUALISATION ################################################

@st.cache_data
def convert_df(df):
    """
    Convertit un DataFrame en CSV (séparateur ;, encodage latin1) et met le résultat en cache pour éviter les recomputations.
    """
    return df.to_csv(sep=";", index=False, encoding="latin1")


def dataviz(df, operateur: str):
    # Gestion de la période temporelle
    df["DATE"] = pd.to_datetime(df["DATE"])
    date_min = df["DATE"].min().strftime("%d/%m/%Y")
    date_max = df["DATE"].max().strftime("%d/%m/%Y")
    logger.info({"date_min": date_min, "date_max": date_max})
    texte = f"ℹ️ La période complète de la FADET s'étend du {date_min} au {date_max}."

    st.markdown(
        f"""
        <div style='text-align: center; color: #ffffff; font-family: "IM Fell French Canon SC", monospace; font-size: 16px;'>
            <p>
                {texte}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    st.markdown(
        """
        <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
            <h2>
                🔍 FILTRES
            </h2>
            <p style='text-align: center; color: #ffffff; font-family: "IM Fell French Canon SC", monospace; font-size: 16px;'>
                Vous pouvez filtrer les données en fonction de la période temporelle, des créneaux horaires, des types d'appels, des correspondants, des adresses et des villes. Les visualisations s’adapteront dynamiquement selon les options que vous choisissez.
                </p>

        </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # --- Définition des colonnes attendues ---
    expected_columns = [
        "DATE",
        "TYPE D'APPEL",
        "ABONNE",
        "CORRESPONDANT",
        "DUREE",
        "IMEI",
        "IMSI",
        "ADRESSE",
        "VILLE",
    ]

    # --- Application des filtres uniquement si le bouton reset n'a pas été cliqué ---
    st.markdown("---")
    col1, col2, col3 = st.columns(3)

    # Copie de travail pour ne pas modifier l'original
    filtered_df = df.copy()
    logger.info({"shape avant filtrage": filtered_df.shape})
    logger.info({"Colonnes avant filtrage": filtered_df.columns.tolist()})

    with col1:
        # Filtres sur les dates
        min_date = filtered_df["DATE"].min()
        max_date = filtered_df["DATE"].max()
        start_date = st.date_input(
            "DATE DE DÉBUT :",
            min_value=min_date,
            max_value=max_date,
            value=min_date,
            key="start_date",
        )
        end_date = st.date_input(
            "DATE DE FIN :",
            min_value=min_date,
            max_value=max_date,
            value=max_date,
            key="end_date",
        )
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        filtered_df = filtered_df[
            (filtered_df["DATE"] >= start_date) & (filtered_df["DATE"] <= end_date)
        ]

        # Filtre de créneau horaire
        filtered_df["HEURE"] = (
            pd.to_numeric(filtered_df["HEURE"], errors="coerce").fillna(-1).astype(int)
        )
        heure_debut, heure_fin = st.slider(
            "CRENEAU HORAIRES :", 0, 23, (0, 23), key="heure_slider"
        )
        filtered_df = filtered_df[
            (filtered_df["HEURE"] >= heure_debut) & (filtered_df["HEURE"] <= heure_fin)
        ]

        # Filtre sur le jour de la semaine
        jours_semaine = filtered_df["JOUR DE LA SEMAINE"].unique().tolist()
        selected_jours = st.multiselect(
            "JOUR DE LA SEMAINE :", options=jours_semaine, key="jour_semaine"
        )
        if selected_jours:
            filtered_df = filtered_df[
                filtered_df["JOUR DE LA SEMAINE"].isin(selected_jours)
            ]

    with col2:
        expected_columns_filter = [
            "TYPE D'APPEL",
            "CORRESPONDANT",
            "IMEI",
            "IMSI",
        ]
        for col in expected_columns_filter:
            if col in filtered_df.columns:
                options = filtered_df[col].unique().tolist()
                selected = st.multiselect(
                    f"{col} :", options=options, key=f"filter_{col}"
                )
                if selected:
                    filtered_df = filtered_df[filtered_df[col].isin(selected)]

    with col3:
        expected_columns_filter_2 = ["ADRESSE", "VILLE"]
        for col in expected_columns_filter_2:
            if col in filtered_df.columns:
                options = filtered_df[col].unique().tolist()
                selected = st.multiselect(
                    f"{col} :", options=options, key=f"filter_{col}"
                )
                if selected:
                    filtered_df = filtered_df[filtered_df[col].isin(selected)]

    st.markdown("---")
    st.markdown(
        """
            <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
                <h2>
                    📋 DETAIL DES COMMUNICATIONS
                </h2>
            </div>
            """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    filtered_columns = [col for col in expected_columns if col in filtered_df.columns]
    filtered_df_user_view = filtered_df[filtered_columns].copy()
    # Conversion et gestion des erreurs
    filtered_df_user_view["DATE"] = pd.to_datetime(
        filtered_df_user_view["DATE"], errors="coerce", format="%d-%m-%Y %H:%M:%S"
    )
    # Formatage
    filtered_df_user_view["DATE"] = filtered_df_user_view["DATE"].dt.strftime(
        "%d-%m-%Y %H:%M:%S"
    )
    st.markdown(
        f"<b>{filtered_df_user_view.shape[0]} communications affichées. Toutes les heures sont affichées en heure locale de La Réunion (UTC+4). </b>",
        unsafe_allow_html=True,
    )
    st.dataframe(filtered_df_user_view, width=1700)
    logger.info({"shape après filtrage": filtered_df.shape})
    logger.info({"Colonnes après filtrage": filtered_df.columns.tolist()})

    min_date_str = start_date.strftime('%d/%m/%Y')
    max_date_str = end_date.strftime('%d/%m/%Y')

    # Bouton pour télécharger les données filtrées au format CSV
    csv = convert_df(filtered_df)
    try:
        st.download_button(
            label="Télécharger les données en CSV",
            data=csv,
            file_name="données_complètes.csv",
            mime="text/csv",
            icon="⬇️",
        )
    except:
        st.write("❌ Une erreur est survenue lors du téléchargement des données.")

    st.markdown("---")

    st.markdown(
        """
            <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
                <h2>
                    📱IMEI - IMSI
                </h2>
            </div>
            """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    col3, col4 = st.columns(2)  # Crée deux colonnes
    # Afficher le nombre de communications par IMEI et IMSI
    with col3:
        st.write(f"Communications par IMEI du {min_date_str} au {max_date_str} :")
        imei, shape = count_IMEI(filtered_df)
        st.dataframe(imei, width=600)
        imei_csv = convert_df(imei)
        try:
            st.download_button(
                label="Télécharger les communications par IMEI",
                data=imei_csv,
                file_name= f"communications_par_imei_{min_date_str}_{max_date_str}.csv",
                mime="text/csv",
                icon="⬇️",
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")

        if shape > 1:
            fig = plot_histogram_with_custom_ticks(filtered_df, "IMEI")
            st.plotly_chart(fig)
            button_download_plotly_graphs(fig, "IMEI", min_date_str, max_date_str)

    with col4:
        st.write(f"Communications IMSI du {min_date_str} au {max_date_str} :")
        imsi, shape = count_IMSI(filtered_df)
        st.dataframe(imsi, width=600)

        imsi_csv = convert_df(imsi)
        try:
            st.download_button(
                label="Télécharger les communications par IMSI",
                data=imsi_csv,
                file_name=f"IMSI_{min_date_str}_{max_date_str}.csv",
                mime="text/csv",
                icon="⬇️",
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")

        if shape > 1:
            fig = plot_histogram_with_custom_ticks(filtered_df, "IMSI")
            st.plotly_chart(fig)
            button_download_plotly_graphs(fig, "IMSI", min_date_str, max_date_str)

    st.markdown("---")

    st.markdown(
        """
            <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
                <h2>
                    📅 UTILISATION DE LA LIGNE
                </h2>
            </div>
            """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # --- Première ligne de graphiques (col5/col6) ---
    show_type_fig = True  # Toujours affiché
    show_comm_histo_week = filtered_df["JOUR DE LA SEMAINE"].nunique() > 1

    if show_type_fig or show_comm_histo_week:
        if show_type_fig and show_comm_histo_week:
            col5, col6 = st.columns(2)
            with col5:
                type_fig = count_phone_type(filtered_df)
                st.plotly_chart(type_fig)
                button_download_plotly_graphs(type_fig, "TYPE COMS",min_date_str, max_date_str )
            with col6:
                comm_histo_week = comm_histo_weekday(filtered_df)
                fig = st.plotly_chart(comm_histo_week)
                button_download_plotly_graphs(comm_histo_week, "COMMUNICATIONS PAR JOUR",min_date_str, max_date_str)
        else:
            if show_type_fig:
                type_fig = count_phone_type(filtered_df)
                st.plotly_chart(type_fig, use_container_width=True)
                button_download_plotly_graphs(fig, "TYPE COMS",min_date_str, max_date_str )
            if show_comm_histo_week:
                comm_histo_week = comm_histo_weekday(filtered_df)
                st.plotly_chart(comm_histo_week, use_container_width=True)
                button_download_plotly_graphs(fig, "COMMUNICATIONS PAR JOUR",min_date_str, max_date_str)
    # --- Graphique global ---
    comm_histo_glo = comm_histo_global(filtered_df, min_date_str, max_date_str)
    fig = go.Figure(comm_histo_glo)
    st.plotly_chart(comm_histo_glo, use_container_width=True)
    button_download_plotly_graphs(fig, "COMMUNICATIONS",min_date_str, max_date_str)
    st.markdown("---")

    # --- Deuxième ligne de graphiques (col7/col8) ---
    show_comm_histo_month = filtered_df.MOIS.nunique() > 1
    show_comm_histo_h = filtered_df["HEURE"].nunique() > 1

    if show_comm_histo_month or show_comm_histo_h:
        if show_comm_histo_month and show_comm_histo_h:
            col7, col8 = st.columns(2)
            with col7:
                comm_histo_month = comm_histo_monthly(filtered_df)
                st.plotly_chart(comm_histo_month)
                button_download_plotly_graphs(fig, "COMMUNICATIONS PAR MOIS", min_date_str, max_date_str)
            with col8:
                comm_histo_h = comm_histo_hour(filtered_df)
                st.plotly_chart(comm_histo_h)
                button_download_plotly_graphs(fig, "COMMUNICATIONS PAR HEURE", min_date_str, max_date_str)
        else:
            if show_comm_histo_month:
                comm_histo_month = comm_histo_monthly(filtered_df)
                st.plotly_chart(comm_histo_month, use_container_width=True)
                button_download_plotly_graphs(fig, "COMMUNICATIONS PAR MOIS", min_date_str, max_date_str)
            if show_comm_histo_h:
                comm_histo_h = comm_histo_hour(filtered_df)
                st.plotly_chart(comm_histo_h, use_container_width=True)
                button_download_plotly_graphs(fig, "COMMUNICATIONS PAR HEURE", min_date_str, max_date_str)
    st.markdown("---")

###################         CORRESPONDANTS                  ###################

    st.markdown(
        """
            <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
                <h2>
                    👥 CORRESPONDANTS
                </h2>
            </div>
            """,
        unsafe_allow_html=True,
    )

    st.markdown("---")
    # Afficher le nombre de communications par correspondant (exclusion des n° spéciaux)

    col_corr_1, col_corr_2 = st.columns(2)  # Crée deux colonnes
    with col_corr_1:
        st.write("Nombre de communications par correspondant (exclusion des n° spéciaux):")
        corr = count_corr(filtered_df)
        corr = corr[(corr["CORRESPONDANT"] != "NAN") & (corr["CORRESPONDANT"] != "INDETERMINE")]
        st.dataframe(corr, width=800)

        # Bouton pour télécharger les résultats de corr en CSV
        corr_csv = convert_df(corr)
        try:
            st.download_button(
                label="Télécharger les communications par correspondant",
                data=corr_csv,
                file_name="communications_par_correspondant.csv",
                mime="text/csv",
                icon="⬇️",
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")

    with col_corr_2:
        fig = analyser_top10_correspondants(filtered_df)
        st.plotly_chart(fig)
        button_download_plotly_graphs(fig, "TYPE COMS TOP 10 CORRESPONDANTS", date_min, date_max)

    st.markdown("---")
    # Titre de la section
    st.markdown(
        """
        <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
            <h2>
                📊 ANALYSE PAR CORRESPONDANT (TOP 10)
            </h2>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("---")

    display_top10_individual_histograms(filtered_df)

    st.markdown("---")

######################                LOCALISATION                  ###################

    st.markdown(
        """
            <div style='text-align: center; color: #d8a824; font-family: "IM Fell French Canon SC", monospace; font-size: 18px;'>
                <h2>
                    🌍 LOCALISATION DE LA LIGNE
                </h2>
            </div>
            """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    col12, col13 = st.columns(2)  # Crée deux colonnes
    with col12:
        st.write("Nombre de communications par adresse du relais :")

        adresse_co = adresse_count(filtered_df)

        adresse_co = adresse_co[adresse_co["ADRESSE"] != "NAN"]

        st.dataframe(adresse_co, width=1000)

        adresse_co_csv = convert_df(adresse_co)
        try:
            st.download_button(
                label="Télécharger le nombre de communications par adresse",
                data=adresse_co_csv,
                file_name="communications_par_adresse.csv",
                mime="text/csv",
                icon="⬇️",
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données")

    with col13:
        # Graphique top 10 déclemenchemnts par ville
        city_plot = plot_city_bar(filtered_df)
        st.plotly_chart(city_plot)
        button_download_plotly_graphs(fig, "DECLENCHEMENTS PAR VILLE", date_min, date_max)

    st.markdown("---")

    # Graphique scatter par ville
    if "VILLE" in filtered_df.columns:
        scatter = scatter_plot_ville(filtered_df)
        st.plotly_chart(scatter)
        button_download_plotly_graphs(fig, "LOCALISATION", date_min, date_max)

    st.markdown("---")
    # Cartographie des relais déclenchés selon l'opérateur

    if operateur == "TELCO":
        new_df = adresse_co.merge(filtered_df, on="ADRESSE", how="left")
        # Convertir les colonnes en types appropriés si nécessaire
        new_df["LATITUDE"] = pd.to_numeric(new_df["LATITUDE"], errors="coerce")
        new_df["LONGITUDE"] = pd.to_numeric(new_df["LONGITUDE"], errors="coerce")
        new_df["POURCENTAGE"] = pd.to_numeric(new_df["POURCENTAGE"], errors="coerce")
        new_df["DECLENCHEMENTS"] = pd.to_numeric(
            new_df["DECLENCHEMENTS"], errors="coerce"
        )
        # Supprimer les lignes avec des valeurs manquantes dans les colonnes critiques
        new_df.dropna(
            subset=["LATITUDE", "LONGITUDE", "POURCENTAGE", "DECLENCHEMENTS"],
            inplace=True,
        )
        carto = carto_adresses(new_df)
        if carto is not None:
            st.plotly_chart(carto)

    else:
        adresse_co = adresse_count(filtered_df)
        # Suppression ligne INDETREMINE
        adresse_co = adresse_co[adresse_co["ADRESSE"] != "INDETERMINE"]
        non_found_addresses = []  # Liste pour stocker les adresses non trouvées
        # 1. Géocodage initial
        for idx, row in adresse_co.iterrows():
            adresse = row["ADRESSE"]
            lat, lng = get_coordinates(adresse)
            if lat is not None and lng is not None:
                adresse_co.at[idx, "LATITUDE"] = lat
                adresse_co.at[idx, "LONGITUDE"] = lng
        # 2. Validation des adresses non trouvées
        df_sans_coord = adresse_co[adresse_co["LATITUDE"].isna()].copy()
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        for idx, row in df_sans_coord.iterrows():
            adresse = row["ADRESSE"]
            lat, lng = geocode_google_maps(adresse, api_key=api_key)
            if lat is not None and lng is not None:
                adresse_co.at[idx, "LATITUDE"] = lat
                adresse_co.at[idx, "LONGITUDE"] = lng
            else:
                non_found_addresses.append(adresse)

        carto = carto_adresses(adresse_co)
        if carto is not None:
            st.plotly_chart(carto)
            button_download_plotly_graphs(fig, "CARTOGRAPHIE", date_min, date_max)


        if non_found_addresses:
            st.write("🔴 Adresses non trouvées :")
            for address in non_found_addresses:
                st.markdown(f"• {address}")

    st.markdown("---")

    st.markdown(
        """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playwrite+IN:wght@100..400&display=swap');

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

    # Conteneur centré avec le bouton à l'intérieur
    end_button = st.button("Retour au menu principal")
    if end_button:
        non_found_addresses.clear()
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.switch_page("pages/🚨Infos.py")

