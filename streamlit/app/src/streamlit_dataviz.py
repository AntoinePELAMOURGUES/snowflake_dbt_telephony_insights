import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pyproj import Proj, Transformer
import requests
import streamlit as st

# Fonction pour convertir les coordonnées Gauss-Laborde à WGS84
def gauss_laborde_to_wgs84(x, y):
    try:
        # Définir le système de projection Gauss-Laborde
        gauss_laborde = Proj(proj='tmerc', lat_0=-21.11666667, lon_0=55.53333333, k=1, x_0=160000, y_0=50000)
        # Définir le système de projection WGS84
        wgs84 = Proj(proj='latlong', datum='WGS84')
        # Créer un transformer pour convertir entre les deux systèmes
        transformer = Transformer.from_proj(gauss_laborde, wgs84)
        # Effectuer la transformation
        lon, lat = transformer.transform(x, y)
        return lat, lon
    except Exception as e:
        print(f"Erreur lors de la conversion des coordonnées: {e}")
        return None, None  # Retourner None en cas d'erreur

# Compter le nombre de communications par correspondant
def count_corr(df):
    try:
        # Compter le nombre de contacts par correspondant
        correspondant_counts = df['CORRESPONDANT'].value_counts().reset_index()
        correspondant_counts.columns = ['CORRESPONDANT', 'NBRE COMS']
        # Filtrer pour garder uniquement les correspondants ayant 11 ou 12 caractères
        correspondant_counts = correspondant_counts[correspondant_counts['CORRESPONDANT'].str.len().isin([11, 12])]
        total_contacts = correspondant_counts['NBRE COMS'].sum()
        # Calculer le POURCENTAGE et arrondir à un chiffre après la virgule
        correspondant_counts['POURCENTAGE'] = ((correspondant_counts['NBRE COMS'] / total_contacts) * 100).round(1)
        # Trier par nombre de contacts du plus élevé au plus bas
        correspondant_counts = correspondant_counts.sort_values(by='NBRE COMS', ascending=False)
        return correspondant_counts
    except Exception as e:
        print(f"Erreur lors du comptage des correspondants: {e}")
        return pd.DataFrame()  # Retourner un DataFrame vide en cas d'erreur



def plot_correspondant_bar(df):
    try:
        fig = px.bar(df, x='CORRESPONDANT', y='NBRE COMS',
                     title='Nombre de Communications par Correspondant (Top 10)',
                     hover_data=['POURCENTAGE'],
                     labels={'CORRESPONDANT': 'Correspondant', 'NBRE COMS': 'Nombre de Communications'})
        # Ajustement de la mise en page
        fig.update_layout(
            xaxis_tickangle=-45,  # Inclinaison des labels de l'axe X
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création du graphique à barres: {e}")
        return None


def plot_city_bar(df):
    try:
        # Compter le nombre de contacts par correspondant
        city_counts = df['VILLE'].value_counts().reset_index()
        city_counts.columns = ['VILLE', 'DECLENCHEMENTS']
        city_counts = city_counts[city_counts['VILLE'] != 'INDETERMINE']
        fig = px.bar(city_counts.head(10), x='VILLE', y='DECLENCHEMENTS',
                     title='Nombre de déclenchement par ville (Top 10)',
                     hover_data=['DECLENCHEMENTS'],  # Inclure TOTAL_COMS dans les informations au survol
                     labels={'VILLE': 'Ville', 'DECLENCHEMENTS': 'Déclenchements'})
        # Ajustement de la mise en page
        fig.update_layout(
            xaxis_tickangle=-45,  # Inclinaison des labels de l'axe X
            bargap=0.2  # Espacement entre les barres
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création du graphique à barres: {e}")
        return None

# Compter le nombre de communications par IMEI
def count_IMEI(df):
    try:
        imei_counts = df['IMEI'].value_counts().reset_index()
        imei_counts.columns = ['IMEI', 'NBRE COMS']
        return imei_counts, imei_counts.shape[0]
    except Exception as e:
        print(f"Erreur lors du comptage des IMEI: {e}")
        return pd.DataFrame()

# Compter le nombre de communications par IMSI
def count_IMSI(df):
    try:
        imsi_counts = df['IMSI'].value_counts().reset_index()
        imsi_counts.columns = ['IMSI', 'NBRE COMS']
        return imsi_counts, imsi_counts.shape[0]
    except Exception as e:
        print(f"Erreur lors du comptage des IMSI: {e}")
        return pd.DataFrame()

# Compter le nombre de types d'appel et créer un graphique en secteurs
def count_phone_type(df):
    try:
        type_appel_counts = df["TYPE D'APPEL"].value_counts().reset_index()
        type_appel_counts.columns = ['TYPE D\'APPEL', 'NBRE']
        total_coms = type_appel_counts['NBRE'].sum()  # Calculer le nombre total de communications
        colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']  # Définir les couleurs pour le graphique

        fig = go.Figure(data=[go.Pie(labels=type_appel_counts['TYPE D\'APPEL'],
                                       values=type_appel_counts['NBRE'],
                                       hole=.7)])  # Ajouter l'argument hole pour créer un donut

        fig.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=20,
                          marker=dict(colors=colors, line=dict(color='#000000', width=2)))

        # Ajouter une annotation au centre du donut pour afficher le nombre total de communications
        fig.update_layout(annotations=[dict(text=f'Total<br>{total_coms}', x=0.5, y=0.5, font_size=30, showarrow=False)])

        return fig  # Retourner la figure Plotly
    except Exception as e:
        print(f"Erreur lors du comptage des types d'appel: {e}")
        return go.Figure()  # Retourner une figure vide en cas d'erreur

# Créer un histogramme global du nombre de communications par jour
def comm_histo_global(df):
    try:
        df['DateOnly'] = df['DATE'].dt.date  # Extraire uniquement la date
        daily_counts = df.groupby('DateOnly').size().reset_index(name='Nombre de communications')
        daily_counts['DateOnly'] = pd.to_datetime(daily_counts['DateOnly'])  # Convertir en datetime
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=daily_counts['DateOnly'],
            y=daily_counts['Nombre de communications'],
            marker=dict(line=dict(color='black', width=1))  # Bordure noire autour des barres
        ))
        fig.update_layout(
            title='Nombre de communications par jour',
            xaxis_title='Date',
            yaxis_title='Nombre de communications',
            xaxis=dict(tickformat='%Y-%m-%d'),  # Format des dates sur l'axe x
            showlegend=False  # Masquer la légende si non nécessaire
        )
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme global: {e}")
        return go.Figure()

# Créer un histogramme mensuel du nombre de communications par mois
def comm_histo_monthly(df):
    try:
        fig_monthly = go.Figure()
        fig_monthly.add_trace(go.Histogram(x=df['MOIS'].dropna().astype(str),
                                            histfunc='count',
                                            name='Communications par mois',
                                            marker=dict(line=dict(color='black', width=1))))
        fig_monthly.update_layout(title='Nombre de communications par mois',
                                   xaxis_title='Mois',
                                   yaxis_title='Nombre de communications')
        return fig_monthly
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme mensuel: {e}")
        return go.Figure()

# Créer un histogramme du nombre de communications par jour de la semaine
def comm_histo_weekday(df):
    try:
        # Définir l'ordre des jours de la semaine
        jours_semaine = ['LUNDI', 'MARDI', 'MERCREDI', 'JEUDI', 'VENDREDI', 'SAMEDI', 'DIMANCHE']

        # Compter le nombre de communications par jour
        counts = df['JOUR DE LA SEMAINE'].value_counts().reindex(jours_semaine, fill_value=0)

        # Créer l'histogramme
        fig_weekday = go.Figure()
        fig_weekday.add_trace(go.Bar(x=counts.index,
                                      y=counts.values,
                                      name='Communications par jour de la semaine',
                                      marker=dict(line=dict(color='black', width=1))))

        # Mettre à jour la mise en page
        fig_weekday.update_layout(title='Nombre de communications par jour de la semaine',
                                   xaxis_title='Jour de la semaine',
                                   yaxis_title='Nombre de communications',
                                   xaxis=dict(type='category'))  # Assurez-vous que l'axe X est traité comme une catégorie

        return fig_weekday
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme par jour de la semaine: {e}")
        return go.Figure()

# Créer un histogramme du nombre de communications par heure
def comm_histo_hour(df):
    try:

        fig = px.histogram(df, x='HEURE', title='Nombre de communications par heure de la journée',
                           labels={'HEURE': 'Heure', 'count': 'Nombre de communications'},
                           histnorm='')
        # Ajouter des bordures aux barres
        fig.update_traces(marker=dict(line=dict(color='black', width=1)))
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de l'histogramme par heure: {e}")
        return go.Figure()

# Compter le nombre d'appels par adresse et calculer les pourcentages associés
def adresse_count(df):
    try:
       adresse_counts = df['ADRESSE'].value_counts().reset_index()
       adresse_counts.columns = ['ADRESSE', 'DECLENCHEMENTS']
       adresse_counts.dropna(axis=0, inplace=True)  # Supprimer les lignes sans adresse
       total_contacts = adresse_counts['DECLENCHEMENTS'].sum()
       adresse_counts['POURCENTAGE'] = ((adresse_counts['DECLENCHEMENTS'] / total_contacts) * 100).round(1)
       adresse_counts.sort_values(by='DECLENCHEMENTS', ascending=False, inplace=True)
       return adresse_counts
    except Exception as e:
       print(f"Erreur lors du comptage des adresses: {e}")
       return pd.DataFrame()

# Créer un scatter plot basé sur les villes et les dates
def scatter_plot_ville(df):
    try:
       fig = px.scatter(df, x='DATE', y='VILLE', color='VILLE', title='Localisation en fonction de la date')
       return fig
    except Exception as e:
       print(f"Erreur lors de la création du scatter plot des villes: {e}")
       return go.Figure()  # Retourner une figure vide en cas d'erreur


# Liste pour stocker les adresses non trouvées
non_found_addresses = []

# Fonction pour géocoder une adresse via un service externe (API)
def geocode_address_datagouv(address):
    try:
        url = "https://data.geopf.fr/geocodage/search"
        params = {
            'q': address,
            'limit': 1  # Limiter à 1 résultat
        }
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if data and 'features' in data and len(data['features']) > 0:
                coords = data['features'][0]['geometry']['coordinates']
                return (coords[1], coords[0])  # Retourner (latitude, longitude)
            else:
                non_found_addresses.append(address)  # Ajouter à la liste si non trouvé
                return (None, None)
        else:
            print(f"Erreur lors de la requête : {response.status_code}")
            non_found_addresses.append(address)  # Ajouter à la liste si erreur
            return (None, None)
    except Exception as e:
        print(f"Erreur lors du géocodage de l'adresse '{address}': {e}")
        non_found_addresses.append(address)  # Ajouter à la liste si exception
        return (None, None)

# Créer une carte des adresses pour l'opérateur SRR avec les coordonnées converties depuis Gauss-Laborde à WGS84.
def carto_adresse_srr(df):
    try:
        adress_count = adresse_count(df)  # Compter les adresses
        df_merged = adress_count.merge(df, how='left', left_on="ADRESSE", right_on='ADRESSE')
        # Appliquer la conversion des coordonnées sur chaque ligne
        df_merged[['LATITUDE', 'LONGITUDE']] = df_merged.apply(lambda row: gauss_laborde_to_wgs84(row['COORDONNEE X'], row['COORDONNEE Y']), axis=1, result_type='expand')
        fig = px.scatter_map(df_merged,
                            lat="LATITUDE", lon="LONGITUDE",
                            color="POURCENTAGE", size="DECLENCHEMENTS",
                            hover_name="ADRESSE",
                            size_max=60, zoom=10,
                            color_continuous_scale=px.colors.sequential.Bluered,
                            map_style="carto-positron")

        return fig
    except Exception as e:
        print(f"Erreur lors de la création de la carte des adresses: {e}")
        return go.Figure()

# Créer une carte des adresses pour l'opérateur ORRE.
def carto_adresse_orre(df):
    try:
        fig = px.scatter_map(df,
                            lat="LATITUDE", lon="LONGITUDE",
                            color="POURCENTAGE", size="DECLENCHEMENTS",
                            hover_name="ADRESSE",
                            size_max=60, zoom=10,
                            color_continuous_scale=px.colors.sequential.Bluered,
                            map_style="carto-positron")
        return fig
    except Exception as e:
        print(f"Erreur lors de la création de la carte ORRE: {e}")
        return go.Figure()

# Créer une carte des adresses pour l'opérateur TCOI.
def carto_adresse_tcoi(df):
    try:
        fig = px.scatter_map(
            df,
            lat="LATITUDE",
            lon="LONGITUDE",
            color="POURCENTAGE",
            size="DECLENCHEMENTS",
            hover_name="ADRESSE",
            size_max=60,
            zoom=10,
            color_continuous_scale=px.colors.sequential.Bluered,
            map_style="carto-positron"
        )
        return fig
    except Exception as e:
        st.error(f"Erreur lors de la création de la carte des adresses : {e}")
        return go.Figure()

@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(sep=';', index= False, encoding='latin1')

############################################ VISUALISATION ######################################################

def visualisation_data(df, operateur: str):
    # Gestion de la période temporelle
    if 'DATE' in df.columns:
        df['DATE'] = pd.to_datetime(df['DATE'])
        st.write("ℹ️ La période complète de la FADET s'étend du {} au {}".format(df["DATE"].min(), df["DATE"].max()))
        st.markdown("---")

        st.write("📅 Vous pouvez modifier la période d'analyse ci-dessous :")
        start_date = st.date_input("Date de début", min_value=df["DATE"].min(), max_value=df["DATE"].max(), value=df["DATE"].min())
        end_date = st.date_input("Date de fin", min_value=df["DATE"].min(), max_value=df["DATE"].max(), value=df["DATE"].max())

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        df = df[(df["DATE"] >= start_date) & (df["DATE"] <= end_date)]
        st.markdown("---")
    else:
        st.write("❌ Aucune date n'a été trouvée dans le fichier chargé.")
        return

    # Définition des colonnes à afficher et des options de filtre
    if "ADRESSE" in df.columns and 'VILLE' in df.columns:
        if operateur == "TCOI":
            expected_columns = ['DATE', 'ABONNE', 'CORRESPONDANT', "TYPE D'APPEL", 'DIRECTION', 'DUREE', 'ADRESSE', 'VILLE', 'IMEI', 'IMSI']
        else:
            expected_columns = ['DATE', 'ABONNE', 'CORRESPONDANT', "TYPE D'APPEL", 'DUREE', 'ADRESSE', 'VILLE', 'IMEI', 'IMSI']
        expected_columns_filter = ["TYPE D'APPEL", "CORRESPONDANT", "IMEI", "IMSI", "VILLE", "ADRESSE", "HEURE", "JOUR DE LA SEMAINE"]
    else:
        st.write("❌ Votre fichier ne contient aucune donnée concernant les adresses ou les villes.")
        expected_columns = ['DATE', 'ABONNE', 'CORRESPONDANT', "TYPE D'APPEL", 'DUREE', 'IMEI', 'IMSI']
        expected_columns_filter = ["TYPE D'APPEL", "CORRESPONDANT", "IMEI", "IMSI", "HEURE", "JOUR DE LA SEMAINE"]

    # Interface pour les sélections multiples
    # Interface pour les sélections multiples en deux colonnes
    st.write("Choisissez un ou plusieurs filtres :")
    col1, col2 = st.columns(2)  # Crée deux colonnes

    selected_filters = {}
    filter_columns = [x for x in expected_columns_filter if x != "HEURE"]
    num_cols = len(filter_columns)
    midpoint = num_cols // 2  # Trouve le point médian pour diviser les filtres

    # Première colonne
    with col1:
        for col in filter_columns[:midpoint]:
            selected_filters[col] = st.multiselect(f"Valeurs pour {col} :", options=df[col].dropna().unique(), key=f"filter_{col}")  # Ajout d'une clé unique

    # Deuxième colonne
    with col2:
        for col in filter_columns[midpoint:]:
            selected_filters[col] = st.multiselect(f"Valeurs pour {col} :", options=df[col].dropna().unique(), key=f"filter_{col}")  # Ajout d'une clé unique

    st.markdown("---")

    # Ajout du filtre de créneau horaire
    st.write("Sélectionnez un créneau horaire :")
    heure_debut, heure_fin = st.slider("Heure de début et de fin :", 0, 23, (0, 23))
    st.markdown("---")

    # Application des filtres sélectionnés
    for col, values in selected_filters.items():
        if values:
            df = df[df[col].isin(values)]

    # Application du filtre de créneau horaire
    df['HEURE'] = df['HEURE'].astype(int)  # Assure que la colonne 'HEURE' est de type entier
    df = df[(df['HEURE'] >= heure_debut) & (df['HEURE'] <= heure_fin)]

    st.write("Voici un aperçu des données filtrées :")
    filtered_df = df[expected_columns]
    st.write(filtered_df)

    # Bouton pour télécharger les données filtrées au format CSV
    csv = convert_df(filtered_df)
    try:
        st.download_button(
            label="Télécharger les données en CSV",
            data=csv,
            file_name='données_complètes.csv',
            mime='text/csv',
            icon = "⬇️"
        )
    except:
        st.write("❌ Une erreur est survenue lors du téléchargement des données.")

    st.markdown("---")

    # Afficher le nombre de communications par correspondant (exclusion des n° spéciaux)
    if 'CORRESPONDANT' in df.columns:
        st.write("Nombre de communications par correspondant (exclusion des n° spéciaux):")
        corr = count_corr(df)
        st.write(corr)

        # Bouton pour télécharger les résultats de corr en CSV
        corr_csv = convert_df(corr)
        try:
            st.download_button(
                label="Télécharger les communications par correspondant",
                data=corr_csv,
                file_name='communications_par_correspondant.csv',
                mime='text/csv',
                icon = "⬇️"
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")
        # st.markdown("---")
        # st.write("📊 Voici un graphique des 10 correspondants les plus fréquents :")
        # top_10_histo = plot_correspondant_bar(corr.head(10))
        # st.plotly_chart(top_10_histo)
    else:
        st.write("❌ La colonne 'Correspondant' n'est pas disponible.")

    st.markdown("---")

    # Afficher le type de communications
    if "TYPE D'APPEL" in df.columns:
        st.write("Type de communications :")
        type_fig = count_phone_type(df)
        st.plotly_chart(type_fig)

        # Optionnel : Ajouter un bouton pour télécharger une image du graphique, si nécessaire.
    else:
        st.write("❌ La colonne 'Type d'appel' n'est pas disponible.")

    # Afficher le nombre de communications par IMEI et IMSI
    if 'IMEI' in df.columns:
        st.markdown("---")
        st.write("Nombre de communications par IMEI :")
        imei, shape = count_IMEI(df)
        st.write(imei)

        imei_csv = convert_df(imei)
        try:
            st.download_button(
                label="Télécharger les communications par IMEI",
                data=imei_csv,
                file_name='communications_par_imei.csv',
                mime='text/csv',
                icon = "⬇️"
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")

        if shape > 1:
            total_days = (df['DATE'].max() - df['DATE'].min()).days
            fig = px.histogram(df, x="DATE", color = "IMEI", nbins=total_days, title="Répartition IMEI sur la période")
            fig.update_layout(bargap=0.01)
            st.plotly_chart(fig)
    else:
        st.write("❌ La colonne 'IMEI' n'est pas disponible.")

    if 'IMSI' in df.columns:
        st.markdown("---")
        st.write("Nombre de communications par IMSI :")
        imsi, shape = count_IMSI(df)
        st.write(imsi)

        imsi_csv = convert_df(imsi)
        try:
            st.download_button(
                label="Télécharger les communications par IMSI",
                data=imsi_csv,
                file_name='communications_par_imsi.csv',
                mime='text/csv',
                icon = "⬇️"
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données.")

        if shape > 1:
            total_days = (df['DATE'].max() - df['DATE'].min()).days
            fig = px.histogram(df, x="DATE", color = "IMSI", nbins=total_days, title="Répartition IMSI sur la période")
            fig.update_layout(bargap=0.01)
            st.plotly_chart(fig)

    else:
        st.write("❌ La colonne 'IMSI' n'est pas disponible.")

    st.markdown("---")
    # Histogrammes des communications
    if not df.empty:  # Vérifier que le DataFrame n'est pas vide avant d'afficher les histogrammes
        comm_histo_glo = comm_histo_global(df)
        st.plotly_chart(comm_histo_glo)
        if df.MOIS.nunique() > 1:
            comm_histo_month = comm_histo_monthly(df)
            st.plotly_chart(comm_histo_month)

        if df['JOUR DE LA SEMAINE'].nunique() > 1:
            comm_histo_week = comm_histo_weekday(df)
            st.plotly_chart(comm_histo_week)

        if df["HEURE"].nunique() > 1:
            comm_histo_h = comm_histo_hour(df)
            st.plotly_chart(comm_histo_h)

    # Nombre de communications par adresse du relais
    if 'ADRESSE' in df.columns:
        st.markdown("---")
        st.write("Nombre de communications par adresse du relais :")

        adresse_co = adresse_count(df)

        st.write(adresse_co)

        adresse_co_csv = convert_df(adresse_co)
        try:
            st.download_button(
                label="Télécharger le nombre de communications par adresse",
                data=adresse_co_csv,
                file_name='communications_par_adresse.csv',
                mime='text/csv',
                icon = "⬇️"
            )
        except:
            st.write("❌ Une erreur est survenue lors du téléchargement des données")

    else:
        st.write("❌ La colonne 'Adresse' n'est pas disponible.")

    st.markdown("---")

    # Graphique top 10 déclemenchemnts par ville
    city_plot = plot_city_bar(df)
    st.plotly_chart(city_plot)

    st.markdown('---')

    # Graphique scatter par ville
    if 'VILLE' in df.columns:
        scatter = scatter_plot_ville(df)
        st.plotly_chart(scatter)

    st.markdown("---")
    # Cartographie des relais déclenchés selon l'opérateur

    if operateur == "TCOI":
        new_df = adresse_co.merge(df, on='ADRESSE', how='left')
        # Convertir les colonnes en types appropriés si nécessaire
        new_df['LATITUDE'] = pd.to_numeric(new_df['LATITUDE'], errors='coerce')
        new_df['LONGITUDE'] = pd.to_numeric(new_df['LONGITUDE'], errors='coerce')
        new_df['POURCENTAGE'] = pd.to_numeric(new_df['POURCENTAGE'], errors='coerce')
        new_df['DECLENCHEMENTS'] = pd.to_numeric(new_df['DECLENCHEMENTS'], errors='coerce')
        # Supprimer les lignes avec des valeurs manquantes dans les colonnes critiques
        new_df.dropna(subset=['LATITUDE', 'LONGITUDE', 'POURCENTAGE', 'DECLENCHEMENTS'], inplace=True)
        carto = carto_adresse_tcoi(new_df)
        if carto is not None:
            st.plotly_chart(carto)

    else :
        if 'ADRESSE' in df.columns:
            adresse_co['Coordinates'] = adresse_co['ADRESSE'].apply(geocode_address_datagouv)
            adresse_co[['LATITUDE', 'LONGITUDE']] = pd.DataFrame(adresse_co['Coordinates'].tolist(), index=adresse_co.index)
            carto = carto_adresse_orre(adresse_co)
            if carto is not None:
                st.plotly_chart(carto)

            if non_found_addresses:
                st.write("🔴 Adresses non trouvées :")
                for address in non_found_addresses:
                    st.markdown(f"• {address}")

    # Bouton pour retourner au menu principal
    if st.button("Retour au menu principal"):
        non_found_addresses.clear()  # Effacer la liste des adresses non trouvées
        for key in list(st.session_state.keys()):
            del st.session_state[key]  # Supprime toutes les clés dans session_state
        # Naviguer vers la page du menu principal
        st.switch_page("pages/menu.py")  # Retour au menu principal