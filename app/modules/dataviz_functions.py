import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import requests
import streamlit as st
import os
import time
from dotenv import load_dotenv
import io
from typing import Tuple
from modules.dashboard_streamlit import *
from modules.dashboard_streamlit_no_geoloc import *

load_dotenv()

# Chargement des variables d'environnement

api_key = os.getenv("GOOGLE_MAPS_API_KEY")
mapbox_token = os.getenv("MAPBOX_TOKEN")
if not api_key or not mapbox_token:
    raise ValueError(
        "Les clés API Google Maps et Mapbox sont requises. Veuillez les définir dans le fichier .env."
    )


#############"      IMEI - IMSI"        #############


def count_IMEI(df):
    """
    Compte le nombre de communications par IMEI dans le DataFrame fourni.
    Retourne un DataFrame des IMEI et leur nombre de communications, ainsi que le nombre total d'IMEI uniques.
    """
    try:
        imei_counts = df["IMEI"].value_counts().reset_index()
        imei_counts.columns = ["IMEI", "NBRE COMS"]
        imei_counts = imei_counts.reset_index(drop=True)
        return imei_counts, imei_counts.shape[0]
    except Exception as e:
        print(f"Erreur lors du comptage des IMEI: {e}")
        return pd.DataFrame()


def count_IMSI(df):
    """
    Compte le nombre de communications par IMSI dans le DataFrame fourni.
    Retourne un DataFrame des IMSI et leur nombre de communications, ainsi que le nombre total d'IMSI uniques.
    """
    try:
        imsi_counts = df["IMSI"].value_counts().reset_index()
        imsi_counts.columns = ["IMSI", "NBRE COMS"]
        imsi_counts = imsi_counts.reset_index(drop=True)
        return imsi_counts, imsi_counts.shape[0]
    except Exception as e:
        print(f"Erreur lors du comptage des IMSI: {e}")
        return pd.DataFrame()


def plot_histogram_with_custom_ticks(filtered_df, column):
    total_days = (filtered_df["DATE"].max() - filtered_df["DATE"].min()).days
    fig = px.histogram(
        filtered_df,
        x="DATE",
        labels={"DATE": "Date"},
        color=column,
        nbins=total_days
    )

    # Détermine la fréquence des ticks
    if total_days < 7:
        tick_freq = 'D'
    elif 7 <= total_days < 30:
        tick_freq = '5D'
    else:
        tick_freq = '10D'

    # Génère la liste des dates pour les ticks
    tickvals = pd.date_range(filtered_df["DATE"].min(), filtered_df["DATE"].max(), freq=tick_freq)
    ticktext = [d.strftime('%d/%m/%Y') for d in tickvals]

    fig.update_layout(
        title={
            "text": f"Répartition {column} sur la période",
            "x": 0.5,
            "xanchor": "center"
        },
        bargap=0.01,
        title_font_size=18,
        legend_font_size=16,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16
    )
    fig.update_xaxes(
        tickmode='array',
        tickvals=tickvals,
        ticktext=ticktext,
        tickangle=45,
        tickfont=dict(size=10)
    )
    fig.update_yaxes(title_text="Nombre de communications")
    return fig



###############     TYPE D'APPEL   #################


def count_phone_type(df):
    """
    Compte le nombre de communications par type d'appel et génère un graphique en secteurs (donut) Plotly.
    Retourne la figure Plotly.
    """
    try:
        type_appel_counts = df["TYPE D'APPEL"].value_counts().reset_index()
        type_appel_counts.columns = ["TYPE D'APPEL", "NBRE"]
        total_coms = type_appel_counts["NBRE"].sum()
        colors = ["gold", "mediumturquoise", "darkorange", "lightgreen"]
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=type_appel_counts["TYPE D'APPEL"],
                    values=type_appel_counts["NBRE"],
                    hole=0.7,
                )
            ]
        )
        fig.update_traces(
            hoverinfo="label+percent",
            textinfo="value",
            textfont_size=20,
            marker=dict(colors=colors, line=dict(color="#000000", width=2)),
        )
        fig.update_layout(
            annotations=[
                dict(
                    text=f"Total<br>{total_coms}",
                    x=0.5,
                    y=0.5,
                    font_size=30,
                    showarrow=False,
                )
            ],
            title={"text": "TYPE DE COMMUNICATIONS", "x": 0.5, "xanchor": "center"},
            bargap=0.01,
        )
        return fig
    except Exception as e:
        print(f"Erreur lors du comptage des types d'appel: {e}")
        return go.Figure()


##################   HISTOGRAMMES   ##################


def comm_histo_global(df, date_min, date_max):
    """
    Crée un histogramme Plotly du nombre de communications par jour sur l'ensemble de la période.
    Retourne la figure Plotly.
    """
    try:
        df["DateOnly"] = df["DATE"].dt.date
        daily_counts = (
            df.groupby("DateOnly").size().reset_index(name="Nombre de communications")
        )
        daily_counts["DateOnly"] = pd.to_datetime(daily_counts["DateOnly"])
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=daily_counts["DateOnly"],
                y=daily_counts["Nombre de communications"],
                marker=dict(line=dict(color="black", width=1)),
            )
        )
        fig.update_layout(
            title={
                "text": f"NOMBRE DE COMMUNICATIONS PAR JOURS DU {date_min} AU {date_max}",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="Date",
            yaxis_title="Nombre de communications",
            xaxis=dict(
                tickformat="%d-%m-%Y",
                range=[daily_counts["DateOnly"].min(), daily_counts["DateOnly"].max()],
            ),
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme global: {e}")
        return go.Figure()


def comm_histo_monthly(df):
    """
    Crée un histogramme Plotly du nombre de communications par mois.
    Retourne la figure Plotly.
    """
    try:
        fig_monthly = go.Figure()
        fig_monthly.add_trace(
            go.Histogram(
                x=df["MOIS"].dropna().astype(str),
                histfunc="count",
                name="Communications par mois",
                marker=dict(line=dict(color="black", width=1)),
            )
        )
        fig_monthly.update_layout(
            title={
                "text": "NOMBRE DE COMMUNICATIONS PAR MOIS",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="Mois",
            yaxis_title="Nombre de communications",
        )
        return fig_monthly
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme mensuel: {e}")
        return go.Figure()


def comm_histo_weekday(df):
    """
    Crée un histogramme Plotly du nombre de communications par jour de la semaine.
    Retourne la figure Plotly.
    """
    try:
        jours_semaine = [
            "LUNDI",
            "MARDI",
            "MERCREDI",
            "JEUDI",
            "VENDREDI",
            "SAMEDI",
            "DIMANCHE",
        ]
        counts = (
            df["JOUR DE LA SEMAINE"].value_counts().reindex(jours_semaine, fill_value=0)
        )
        fig_weekday = go.Figure()
        fig_weekday.add_trace(
            go.Bar(
                x=counts.index,
                y=counts.values,
                name="Communications par jour de la semaine",
                marker=dict(line=dict(color="black", width=1)),
            )
        )
        fig_weekday.update_layout(
            title={
                "text": "NOMBRE DE COMMUNICATIONS PAR JOUR DE LA SEMAINE",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_title="Jour de la semaine",
            yaxis_title="Nombre de communications",
            xaxis=dict(type="category"),
        )
        return fig_weekday
    except Exception as e:
        print(
            f"Erreur lors de la création de l'histogramme par jour de la semaine: {e}"
        )
        return go.Figure()


def comm_histo_hour(df):
    """
    Crée un histogramme Plotly du nombre de communications par heure de la journée.
    Retourne la figure Plotly.
    """
    try:
        fig = px.histogram(
            df,
            x="HEURE",
            labels={"HEURE": "Heure", "count": "Nombre de communications"},
            histnorm="",
            range_x=[0, 24],
        )
        fig.update_traces(marker=dict(line=dict(color="black", width=1)))
        fig.update_layout(
            title={
                "text": "NOMBRE DE COMMUNICATIONS PAR HEURE DE LA JOURNEE",
                "x": 0.5,
                "xanchor": "center",
            }
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme par heure: {e}")
        return go.Figure()


##################    CORRESPONDANTS    ##################


def count_corr(df):
    """
    Compte le nombre de communications par correspondant, calcule les pourcentages et les dates du premier et dernier appel.
    Retourne un DataFrame détaillé avec les informations par correspondant.
    """
    try:
        df = df.copy()
        df["CORRESPONDANT"] = df["CORRESPONDANT"].astype(str)
        df = df[df["CORRESPONDANT"].str.len().isin([11, 12])]
        grouped = (
            df.groupby("CORRESPONDANT")
            .agg(
                NBRE_COMS=("CORRESPONDANT", "size"),
                DATE_PREMIER_APPEL=("DATE", "min"),
                DATE_DERNIER_APPEL=("DATE", "max"),
            )
            .reset_index()
        )
        correspondant_counts = grouped.rename(columns={"NBRE_COMS": "NBRE COMS"})
        total_contacts = correspondant_counts["NBRE COMS"].sum()
        correspondant_counts["POURCENTAGE"] = (
            (correspondant_counts["NBRE COMS"] / total_contacts) * 100
        ).round(1)
        correspondant_counts = correspondant_counts.sort_values(
            "NBRE COMS", ascending=False
        )
        correspondant_counts["DATE_PREMIER_APPEL"] = correspondant_counts[
            "DATE_PREMIER_APPEL"
        ].dt.strftime("%d-%m-%Y")
        correspondant_counts["DATE_DERNIER_APPEL"] = correspondant_counts[
            "DATE_DERNIER_APPEL"
        ].dt.strftime("%d-%m-%Y")
        return correspondant_counts[
            [
                "CORRESPONDANT",
                "NBRE COMS",
                "POURCENTAGE",
                "DATE_PREMIER_APPEL",
                "DATE_DERNIER_APPEL",
            ]
        ]
    except Exception as e:
        print(f"Erreur lors du comptage des correspondants: {e}")
        return pd.DataFrame(
            columns=[
                "CORRESPONDANT",
                "NBRE COMS",
                "POURCENTAGE",
                "DATE_PREMIER_APPEL",
                "DATE_DERNIER_APPEL",
            ]
        )

def analyser_top10_correspondants(df):
    """
    Crée un graphique des 10 correspondants les plus contactés avec
    une répartition par type d'appel, en filtrant les numéros ayant entre 11 et 12 caractères.

    Paramètres:
    df (DataFrame): DataFrame contenant au moins les colonnes 'CORRESPONDANT' et "TYPE D'APPEL"

    Retourne:
    plotly.Figure: Le graphique des top 10 correspondants
    """
    # Convertir les correspondants en chaînes de caractères
    df['CORRESPONDANT'] = df['CORRESPONDANT'].astype(str)

    # Filtrer les correspondants avec des numéros de 11 à 12 caractères
    df_filtre = df[df['CORRESPONDANT'].str.len().between(11, 12)]
    df_filtre = df_filtre[(df_filtre["CORRESPONDANT"] != "NAN") & (df_filtre["CORRESPONDANT"] != "INDETERMINE")]

    # Comptage du nombre d'appels par correspondant et type
    df_counts = df_filtre.groupby(['CORRESPONDANT', "TYPE D'APPEL"]).size().reset_index(name='nombre appels')

    # Calcul du total des appels par correspondant
    total_appels = df_counts.groupby('CORRESPONDANT')['nombre appels'].sum().reset_index(name='total appels')

    # Récupérer les 10 correspondants les plus contactés
    top10 = total_appels.sort_values(by='total appels', ascending=False).head(10)

    # Filtrer le DataFrame pour ne garder que ces 10 correspondants
    filtered_df = df_counts[df_counts['CORRESPONDANT'].isin(top10['CORRESPONDANT'])]

    # Création du graphique Plotly
    fig = px.bar(
        filtered_df,
        x='CORRESPONDANT',
        y='nombre appels',
        color="TYPE D'APPEL",
        labels={
            'CORRESPONDANT': 'Correspondant',
            'nombre appels': "Nombre d'appels",
            "TYPE D'APPEL": "Type d'appel"
        }
    )
    # Forcer l'axe X à être catégoriel
    fig.update_xaxes(type='category')  # <-- clé pour l'affichage correct[4][5]

    fig.update_layout(
            title={
                "text": "TYPE DE COMMUNICATIONS TOP 10",
                "x": 0.5,
                "xanchor": "center",
            }
        )

    return fig


def display_top10_individual_histograms(df):
    """
    Affiche, dans deux colonnes Streamlit, les histogrammes quotidiens des communications pour les 10 correspondants les plus contactés.
    Chaque graphique couvre toute la période, même les jours sans communication.
    """
    df = df.copy()
    df["DATE"] = pd.to_datetime(df["DATE"])
    df["CORRESPONDANT"] = df["CORRESPONDANT"].astype(str)
    df = df[df["CORRESPONDANT"].str.len().isin([11, 12])]
    df = df[(df["CORRESPONDANT"] != "NAN") & (df["CORRESPONDANT"] != "INDETERMINE")]
    # Période globale
    date_min = df["DATE"].dt.date.min()
    date_max = df["DATE"].dt.date.max()
    all_dates = pd.date_range(date_min, date_max, freq="D").date

    # Top 10 correspondants
    top10 = df["CORRESPONDANT"].value_counts().head(10).index.tolist()
    df_top10 = df[df["CORRESPONDANT"].isin(top10)]

    # Grouper par correspondant et date, compter les communications
    df_top10["DATE_ONLY"] = df_top10["DATE"].dt.date
    grouped = (
        df_top10.groupby(["CORRESPONDANT", "DATE_ONLY"])
        .size()
        .reset_index(name="NBRE_COMS")
    )

    # Créer et afficher les graphiques sur 2 colonnes
    col10, col11 = st.columns(2)

    for i, corr in enumerate(top10):
        current_col = col10 if i % 2 == 0 else col11

        with current_col:
            # Générer toutes les dates de la période pour ce correspondant
            df_corr = grouped[grouped["CORRESPONDANT"] == corr].set_index("DATE_ONLY")
            df_corr = df_corr.reindex(all_dates, fill_value=0).reset_index()
            df_corr.rename(columns={"index": "DATE_ONLY"}, inplace=True)
            df_corr["CORRESPONDANT"] = corr  # pour cohérence

            # Création de l'histogramme
            fig = px.bar(
                df_corr,
                x="DATE_ONLY",
                y="NBRE_COMS",
                labels={"DATE_ONLY": "Date", "NBRE_COMS": "Nombre de coms"},
                color_discrete_sequence=["#2c7be5"],
            )

            # Personnalisation du survol et format
            fig.update_traces(
                hovertemplate="Date: %{x|%d-%m-%Y}<br>Nombre de coms: %{y}",
                marker_line_color="rgba(0,0,0,0.5)",
                marker_line_width=1,
            )

            # Ajustements de mise en page
            fig.update_layout(
                xaxis_tickformat="%d-%m-%Y",
                margin=dict(l=10, r=10, t=40, b=20),
                height=300,
                plot_bgcolor="rgba(0,0,0,0.02)",
                xaxis=dict(range=[pd.to_datetime(date_min), pd.to_datetime(date_max)]),
                title={
                    "text": f"COMMUNICATION(S) AVEC LA LIGNE DU CORRESPONDANT {corr}",
                    "x": 0.5,
                    "xanchor": "center",
                },
            )

            st.plotly_chart(fig, use_container_width=True)
            button_download_plotly_graphs(fig, f"Coms avec {corr} du", date_min, date_max)


def plot_correspondant_bar(df):
    """
    Génère un graphique à barres Plotly du nombre de communications pour chaque correspondant (Top 10).
    Retourne la figure Plotly.
    """
    try:
        fig = px.bar(
            df,
            x="CORRESPONDANT",
            y="NBRE COMS",
            title="Nombre de Communications par Correspondant (Top 10)",
            hover_data=["POURCENTAGE"],
            labels={
                "CORRESPONDANT": "Correspondant",
                "NBRE COMS": "Nombre de Communications",
            },
        )
        fig.update_layout(
            xaxis_tickangle=-45,
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création du graphique à barres: {e}")
        return None


##############"  GEOLOCALISATION ##############"


def adresse_count(df):
    """
    Compte le nombre d'appels par adresse, calcule les pourcentages associés et trie les résultats.
    Retourne un DataFrame avec adresse, nombre de déclenchements et pourcentage.
    """
    try:
        df = df[(df["ADRESSE"] != "NAN") & (df["ADRESSE"] != "INDETERMINE")]
        adresse_counts = df["ADRESSE"].value_counts().reset_index()
        adresse_counts.columns = ["ADRESSE", "DECLENCHEMENTS"]
        adresse_counts.dropna(axis=0, inplace=True)
        total_contacts = adresse_counts["DECLENCHEMENTS"].sum()
        adresse_counts["POURCENTAGE"] = (
            (adresse_counts["DECLENCHEMENTS"] / total_contacts) * 100
        ).round(1)
        adresse_counts.sort_values(by="DECLENCHEMENTS", ascending=False, inplace=True)
        return adresse_counts
    except Exception as e:
        print(f"Erreur lors du comptage des adresses: {e}")
        return pd.DataFrame()


def plot_city_bar(df):
    """
    Génère un graphique à barres Plotly du nombre de déclenchements par ville (hors 'INDETERMINE').
    Retourne la figure Plotly.
    """
    try:
        city_counts = df["VILLE"].value_counts().reset_index()
        city_counts.columns = ["VILLE", "DECLENCHEMENTS"]
        city_counts = city_counts[(city_counts["VILLE"] != "INDETERMINE") & (city_counts["VILLE"] != "NAN")]
        fig = px.bar(
            city_counts,
            x="VILLE",
            y="DECLENCHEMENTS",
            hover_data=["DECLENCHEMENTS"],
            labels={"VILLE": "Ville", "DECLENCHEMENTS": "Déclenchements"},
        )
        fig.update_layout(
            title={
                "text": "NOMBRE DE DECLENCHEMENT PAR VILLE",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis_tickangle=-45,
            bargap=0.2,
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création du graphique à barres: {e}")
        return None


def scatter_plot_ville(df):
    """
    Crée un nuage de points Plotly représentant la localisation des lignes par ville et par date.
    Retourne la figure Plotly.
    """
    try:
        df = df.dropna(subset=["VILLE"])
        df = df[(df["VILLE"] != "INDETERMINE") & (df["VILLE"] != "NAN")]
        df['HEURE_PRECISE'] = df['DATE'].dt.strftime('%H:%M:%S')
         # On crée une colonne formatée pour l'affichage dans le tooltip
        fig = px.scatter(
            df,
            x="DATE",
            y="VILLE",
            color="VILLE",
            hover_data=["HEURE_PRECISE"]
        )
        fig.update_layout(
            title={
                "text": "LOCALISATION DE LA LIGNE SUR LA PERIODE ETUDIEE",
                "x": 0.5,
                "xanchor": "center",
            },
            xaxis=dict(
                tickformat="%d-%m-%Y", range=[df["DATE"].min(), df["DATE"].max()]
            ),
            bargap=0.2,
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création du scatter plot des villes: {e}")
        return go.Figure()


############## FONCTIONS CARTOGRAPHIE ##############

# Liste pour stocker les adresses non trouvées
non_found_addresses = []


def get_coordinates(address, api_key=None):  # api_key optionnel pour compatibilité
    """
    Retourne (lat, lng) pour une adresse via data.geopf.fr, ou (None, None) si non trouvée.
    """
    try:
        url = "https://data.geopf.fr/geocodage/search"
        params = {"q": address, "limit": 1}

        response = requests.get(url, params=params)
        response.raise_for_status()  # Lève une exception pour les codes 4xx/5xx

        data = response.json()
        features = data.get("features", [])

        if features:
            coords = features[0]["geometry"]["coordinates"]
            return (coords[1], coords[0])  # (latitude, longitude)
        else:
            print(f"Aucun résultat pour l'adresse : {address}")
            return (None, None)

    except requests.exceptions.RequestException as e:
        print(f"Erreur réseau lors du géocodage de '{address}' : {e}")
        return (None, None)
    except (KeyError, IndexError) as e:
        print(f"Structure de réponse inattendue pour '{address}' : {e}")
        return (None, None)


def geocode_google_maps(adresse, api_key=None):
    """
    Géocode une adresse à La Réunion via l'API Google Address Validation.
    Retourne (latitude, longitude) ou (None, None) en cas d'échec.
    """
    # Validation de la clé API
    if not api_key:
        raise ValueError("Clé API Google Maps requise.")

    # Configuration de l'API
    url = f"https://addressvalidation.googleapis.com/v1:validateAddress?key={api_key}"

    # Configuration spécifique à La Réunion
    payload = {
        "address": {
            "addressLines": [adresse],  # Doit être une liste même pour une seule ligne
            "regionCode": "FR",  # Code région pour La Réunion (au lieu de FR)
        },
        "enableUspsCass": False,
    }

    try:
        # Respect du rate limiting (50 req/s)
        time.sleep(0.02)  # 20 ms entre les requêtes

        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        # Extraction des coordonnées avec vérifications en cascade
        if data.get("result"):
            geocode = data["result"].get("geocode", {})
            location = geocode.get("location", {})
            return location.get("latitude"), location.get("longitude")

        return None, None

    except requests.exceptions.RequestException as e:
        print(f"Erreur réseau/API pour '{adresse}' : {str(e)}")
    except KeyError as e:
        print(f"Champ manquant dans la réponse pour '{adresse}' : {str(e)}")

    return None, None


def carto_adresses(df):
    """
    Crée une carte Plotly Mapbox représentant les adresses avec leur nombre de déclenchements et pourcentage.
    Retourne la figure Plotly.
    """
    try:
        fig = px.scatter_mapbox(
            df,
            lat="LATITUDE",
            lon="LONGITUDE",
            color="POURCENTAGE",
            size="DECLENCHEMENTS",
            hover_name="ADRESSE",
            size_max=60,
            zoom=8,
            color_continuous_scale=px.colors.sequential.Bluered,
            mapbox_style="mapbox://styles/mapbox/outdoors-v12",
            height=800,
        )
        fig.update_layout(mapbox_accesstoken=mapbox_token,
                          mapbox_center={"lat": -21.115141, "lon": 55.536384},
                          mapbox_zoom=9)
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de la carte ORRE: {e}")
        return go.Figure()

@st.cache_data
def convert_df(df):
    """
    Convertit un DataFrame en CSV (séparateur ;, encodage latin1) et met le résultat en cache pour éviter les recomputations.
    """
    return df.to_csv(sep=";", index=False, encoding="latin1")

#########         SAUVEGARDES        ###########

def button_download_plotly_graphs(fig, filename, date_min, date_max):
    img_buffer = io.BytesIO()
    fig.write_image(img_buffer, format="png")
    img_buffer.seek(0)  # Remets le curseur au début du buffer
    # Bouton de téléchargement sous le graphique
    st.download_button(
        label= f"{filename} du {date_min} au {date_max}",
        data=img_buffer,
        file_name= f"{filename}_{date_min}_{date_max}",
        mime="image/png",
        key=filename,

        icon="⬇️")
