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


def no_loc_dataviz(df):
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

    st.write("Nombre de communications par correspondant (exclusion des n° spéciaux):")
    corr = count_corr(filtered_df)
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