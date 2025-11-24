import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark import Session
import datetime

# ==============================================================================
# 1. CONFIGURATION & CONNEXION
# ==============================================================================
st.set_page_config(
    page_title="Analyse Individuelle",
    page_icon="🕵️‍♂️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Fonction de connexion (Pattern Singleton)
@st.cache_resource
def create_snowpark_session():
    try:
        return Session.builder.configs(st.secrets["snowflake"]).create()
    except Exception as e:
        st.error(f"Erreur connexion Snowflake: {e}")
        return None


session = create_snowpark_session()

# --- VÉRIFICATION DU CONTEXTE ---
# On vérifie qu'on arrive bien depuis la page de gestion avec un fichier sélectionné
if (
    "analysis_context" not in st.session_state
    or not st.session_state["analysis_context"]
):
    st.warning(
        "Aucun fichier sélectionné. Veuillez passer par la page 'Gestion Dossiers'."
    )
    if st.button("Retourner aux Dossiers"):
        st.switch_page("pages/Gestion_Dossiers.py")
    st.stop()

# Récupération des métadonnées du fichier sélectionné (le premier de la liste)
context = st.session_state["analysis_context"][0]
CURRENT_DOSSIER_ID = context["DOSSIER_ID"]
CURRENT_FILENAME = context["FILENAME"]
TARGET_NAME = context.get("TARGET_NAME", "Cible Inconnue")
TARGET_ID = context.get("TARGET_IDENTIFIER", "N/A")


# ==============================================================================
# 2. CHARGEMENT DES DONNÉES (MARTS)
# ==============================================================================
@st.cache_data(ttl=3600)
def get_communications_data(dossier_id, filename):
    """
    Récupère les données enrichies depuis la table MARTS.PROD.FCT_COMMUNICATIONS
    """
    query = """
        SELECT
            DATE_HEURE_UTC_FR,
            TYPE_COMMUNICATION,
            DIRECTION,
            DUREE_SECONDES,
            MSISDN_CORRESPONDANT,
            NOM_CORRESPONDANT,
            ADRESSE_CELLULE,
            VILLE_CELLULE,
            LATITUDE,
            LONGITUDE,
            NOM_CIBLE,
            IMEI_CIBLE,
            IMSI_CIBLE,
            MSISDN_CIBLE
        FROM MARTS.PROD.FCT_COMMUNICATIONS
        WHERE DOSSIER_ID = ?
        AND SOURCE_FILENAME = ?
        ORDER BY DATE_HEURE_UTC_FR ASC
    """
    df = session.sql(query, params=[dossier_id, filename]).to_pandas()

    # Conversion datetime pour manipulation facile avec Pandas
    df["DATE_HEURE_UTC_FR"] = pd.to_datetime(df["DATE_HEURE_UTC_FR"])

    # Création de colonnes dérivées pour l'analyse temporelle
    df["DATE"] = df["DATE_HEURE_UTC_FR"].dt.date
    df["HEURE"] = df["DATE_HEURE_UTC_FR"].dt.hour
    df["JOUR_SEMAINE"] = df["DATE_HEURE_UTC_FR"].dt.day_name()
    df["VILLE_CELLULE"] = df["VILLE_CELLULE"].fillna("INCONNUE")
    df["ADRESSE_CELLULE"] = df["ADRESSE_CELLULE"].fillna("ADRESSE INCONNUE")
    return df


# Chargement
with st.spinner(f"Chargement des données pour {TARGET_NAME}..."):
    df = get_communications_data(CURRENT_DOSSIER_ID, CURRENT_FILENAME)

# --- SÉCURITÉ : VÉRIFICATION DES DONNÉES ---
if df.empty:
    st.warning("⚠️ Aucune donnée trouvée pour ce fichier dans la base d'analyse.")
    st.info(
        "Cela peut venir de deux causes :\n"
        "1. Le traitement automatique (Airflow/dbt) n'est pas encore terminé.\n"
        "2. Le fichier ne contient aucune communication valide."
    )

    if st.button("🔄 Rafraîchir la page"):
        st.rerun()
    st.stop()  # Arrête l'exécution ici pour éviter le crash

# Vérification spécifique des dates
if df["DATE"].isna().all():
    st.error("❌ Les données existent mais aucune date valide n'a été trouvée.")
    st.stop()
# # ==============================================================================
# 3. SIDEBAR : FILTRES AVANCÉS
# ==============================================================================
with st.sidebar:
    st.header("🛠️ Filtres d'Investigation")
    st.info(f"Dossier : {TARGET_NAME}")

    # A. Filtre Période (Date)
    min_ts = df["DATE"].min()
    max_ts = df["DATE"].max()

    # Gestion cas mono-date
    if pd.isnull(min_ts):
        min_ts = datetime.today().date()
    if pd.isnull(max_ts):
        max_ts = datetime.today().date()

    date_range = st.date_input("📅 Période", [min_ts, max_ts])

    # B. Filtre Horaire (Heure de début / Heure de fin)
    # Utile pour isoler la nuit (ex: 22h - 06h)
    st.write("⏰ Créneau Horaire")
    hour_range = st.slider("Heures", 0, 23, (0, 23))

    # C. Filtre Type
    types = df["TYPE_COMMUNICATION"].unique()
    selected_types = st.multiselect("Type", types, default=types)

    # --- APPLICATION DES FILTRES ---
    # 1. Date
    if len(date_range) == 2:
        mask_date = (df["DATE"] >= date_range[0]) & (df["DATE"] <= date_range[1])
    else:
        mask_date = df["DATE"] == date_range[0]

    # 2. Heure
    mask_hour = (df["HEURE"] >= hour_range[0]) & (df["HEURE"] <= hour_range[1])

    # 3. Type
    mask_type = df["TYPE_COMMUNICATION"].isin(selected_types)

    df_filtered = df[mask_date & mask_hour & mask_type]

    st.divider()
    st.metric("Lignes filtrées", f"{len(df_filtered)} / {len(df)}")
## ==============================================================================
# 4. HEADER & KPIS
# ==============================================================================
st.title(f"🔍 Rapport d'Analyse : {TARGET_NAME}")
st.markdown(
    f"**Identifiant :** `{TARGET_ID}` | **Fichier source :** `{CURRENT_FILENAME}`"
)
st.markdown("---")

# KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Interactions", f"{len(df_filtered):,}")
with col2:
    # Calcul durée totale en heures
    total_hours = df_filtered["DUREE_SECONDES"].fillna(0).sum() / 3600
    st.metric("Durée Cumulée (Appels)", f"{total_hours:.1f} h")
with col3:
    nb_contacts = df_filtered["MSISDN_CORRESPONDANT"].nunique()
    st.metric("Contacts Uniques", f"{nb_contacts}")
with col4:
    # Taux de géolocalisation
    geo_rate = (df_filtered["LATITUDE"].count() / len(df_filtered)) * 100
    st.metric("Taux Géolocalisation", f"{geo_rate:.1f} %")

if geo_rate < 10:
    st.warning(
        "⚠️ Attention : Très peu de données de géolocalisation disponibles dans ce fichier."
    )

# ==============================================================================
# 5. ANALYSE TEMPORELLE (PATTERN DE VIE)
# ==============================================================================
st.subheader("📅 Pattern de Vie")

tab_timeline, tab_heatmap = st.tabs(["Chronologie", "Habitudes (Heatmap)"])

# ... (Début de la section 5 inchangé)

with tab_timeline:
    # 1. Calcul de la durée sélectionnée
    if len(date_range) == 2:
        delta_days = (date_range[1] - date_range[0]).days
    else:
        delta_days = 0  # Cas où une seule date est sélectionnée

    # 2. Logique de Bascule (Seuil à 3 jours)
    if delta_days > 3:
        # CAS A : VUE MACRO (Par Jour)
        activity_counts = (
            df_filtered.groupby(["DATE", "DIRECTION"]).size().reset_index(name="count")
        )
        x_axis = "DATE"
        titre_graph = f"Volume d'activité par JOUR (Période de {delta_days} jours)"
        tick_format = "%d %b"  # Ex: 01 Jan

    else:
        # CAS B : VUE MICRO (Par Heure)
        # On crée une colonne temporaire arrondie à l'heure (ex: 2025-10-01 14:00:00)
        df_filtered["HEURE_FULL"] = df_filtered["DATE_HEURE_UTC_FR"].dt.floor("h")

        activity_counts = (
            df_filtered.groupby(["HEURE_FULL", "DIRECTION"])
            .size()
            .reset_index(name="count")
        )
        x_axis = "HEURE_FULL"
        titre_graph = "Volume d'activité par HEURE (Vue détaillée)"
        tick_format = "%d/%m %Hh"  # Ex: 01/10 14h

    # 3. Création du Graphique Dynamique
    fig_timeline = px.bar(
        activity_counts,
        x=x_axis,
        y="count",
        color="DIRECTION",
        title=titre_graph,
        color_discrete_map={
            "SORTANT": "#EF553B",
            "ENTRANT": "#636EFA",
            "INCONNU": "grey",
        },
    )

    # Optimisation de l'axe X
    fig_timeline.update_xaxes(
        tickformat=tick_format,
        dtick=(
            "D1" if delta_days > 3 else 3600000 * 4
        ),  # Tick chaque jour OU toutes les 4h
    )

    st.plotly_chart(fig_timeline, use_container_width=True)

with tab_heatmap:
    # Matrice Heure vs Jour de la semaine
    # On force l'ordre des jours
    days_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    heatmap_data = (
        df_filtered.groupby(["JOUR_SEMAINE", "HEURE"]).size().reset_index(name="count")
    )

    fig_heatmap = px.density_heatmap(
        heatmap_data,
        x="HEURE",
        y="JOUR_SEMAINE",
        z="count",
        nbinsx=24,
        category_orders={"JOUR_SEMAINE": days_order},
        color_continuous_scale="Viridis",
        title="Densité des communications (Jour/Heure)",
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

with st.expander(
    "📱 Identifiants présents dans le fichier (Période d'activité)", expanded=True
):
    # On compile les MSISDN, IMEI, IMSI uniques avec leur date min et max

    # Fonction helper pour aggréger
    def get_id_stats(col_name, label):
        stats = (
            df_filtered.groupby(col_name)["DATE_HEURE_UTC_FR"]
            .agg(["min", "max", "count"])
            .reset_index()
        )
        stats.columns = ["Identifiant", "Premier Usage", "Dernier Usage", "Volume"]
        stats["Type"] = label
        return stats

    ids_df = pd.concat(
        [
            get_id_stats("MSISDN_CIBLE", "MSISDN (Ligne)"),
            get_id_stats("IMEI_CIBLE", "IMEI (Boîtier)"),
            get_id_stats("IMSI_CIBLE", "IMSI (SIM)"),
        ]
    ).dropna(
        subset=["Identifiant"]
    )  # On vire les nuls

    # Affichage propre
    st.dataframe(
        ids_df,
        column_config={
            "Premier Usage": st.column_config.DatetimeColumn(
                format="D MMM YYYY, HH:mm"
            ),
            "Dernier Usage": st.column_config.DatetimeColumn(
                format="D MMM YYYY, HH:mm"
            ),
        },
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")

# ==============================================================================
# 6. ANALYSE RELATIONNELLE (TOP CONTACTS)
# ==============================================================================
col_left, col_right = st.columns([1, 1])

with col_left:
    # ==============================================================================
    # 6. ANALYSE DES CORRESPONDANTS (TABLEAU DÉTAILLÉ)
    # ==============================================================================
    st.subheader("👥 Répertoire des Correspondants")

    if not df_filtered.empty:
        # Agrégation puissante
        correspondants = (
            df_filtered.groupby("MSISDN_CORRESPONDANT")
            .agg(
                Nom=(
                    "NOM_CORRESPONDANT",
                    "first",
                ),  # On prend le premier nom trouvé (souvent enrichi)
                Nb_Contacts=("DATE_HEURE_UTC_FR", "count"),
                Premier_Contact=("DATE_HEURE_UTC_FR", "min"),
                Dernier_Contact=("DATE_HEURE_UTC_FR", "max"),
                Duree_Totale=("DUREE_SECONDES", "sum"),
            )
            .reset_index()
        )

        # Nettoyage : Si Nom est vide ou null, mettre "NON IDENTIFIÉ"
        correspondants["Nom"] = (
            correspondants["Nom"]
            .fillna("NON IDENTIFIÉ")
            .replace("INCONNU", "NON IDENTIFIÉ")
        )

        # Tri par volume décroissant
        correspondants = correspondants.sort_values("Nb_Contacts", ascending=False)

        st.dataframe(
            correspondants,
            column_config={
                "MSISDN_CORRESPONDANT": "Numéro",
                "Nom": "Identification (Annuaire)",
                "Nb_Contacts": st.column_config.NumberColumn("Volume", format="%d"),
                "Premier_Contact": st.column_config.DatetimeColumn(
                    format="DD/MM/YY HH:mm"
                ),
                "Dernier_Contact": st.column_config.DatetimeColumn(
                    format="DD/MM/YY HH:mm"
                ),
                "Duree_Totale": st.column_config.NumberColumn("Durée (s)"),
            },
            use_container_width=True,
            hide_index=True,
        )

with col_right:
    st.subheader("📊 Répartition")
    # Pie chart Type de com
    pie_data = df_filtered["TYPE_COMMUNICATION"].value_counts().reset_index()
    pie_data.columns = ["Type", "Count"]

    fig_pie = px.pie(
        pie_data,
        values="Count",
        names="Type",
        hole=0.4,
        title="Types de communications",
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ==============================================================================
# 7. ANALYSE GÉOGRAPHIQUE : JOUR vs NUIT (AVEC CARTE NUMÉROTÉE)
# ==============================================================================
st.subheader("🌙 Habitudes : Relais Couchants vs Diurnes")


# --- Fonctions Utilitaires pour cette section ---
def get_top_5_mapped(df_source):
    """
    Génère un Top 5 des adresses avec coordonnées GPS,
    en excluant les adresses inconnues.
    """
    # 1. Filtrage strict : On retire les inconnus et les sans-GPS
    df_clean = df_source[
        (df_source["ADRESSE_CELLULE"] != "ADRESSE INCONNUE")
        & (df_source["ADRESSE_CELLULE"].notna())
        & (df_source["LATITUDE"].notna())
    ]

    if df_clean.empty:
        return pd.DataFrame()

    # 2. Agrégation : On compte le volume ET on garde la première position GPS connue
    # Cela permet d'avoir un point à afficher sur la carte pour cette adresse
    stats = (
        df_clean.groupby("ADRESSE_CELLULE")
        .agg(
            Volume=("DATE_HEURE_UTC_FR", "count"),
            Lat=("LATITUDE", "first"),
            Lon=("LONGITUDE", "first"),
        )
        .reset_index()
    )

    # 3. Tri Descendant et Top 5
    stats = stats.sort_values("Volume", ascending=False).head(5)

    # 4. Création du Rang (1, 2, 3...) pour l'affichage carte/tableau
    stats["Rang"] = range(1, len(stats) + 1)

    return stats


def display_top5_map(df_top):
    """Affiche une mini-carte avec marqueurs numérotés (Version Graph Objects)"""
    if df_top.empty:
        return

    # 1. Nettoyage et Conversion
    df_top["Lat"] = pd.to_numeric(df_top["Lat"], errors="coerce")
    df_top["Lon"] = pd.to_numeric(df_top["Lon"], errors="coerce")
    df_top["Rang_Str"] = df_top["Rang"].astype(str)

    df_map = df_top.dropna(subset=["Lat", "Lon"])

    if df_map.empty:
        st.warning("Données GPS invalides.")
        return

    # 2. Centrage
    center_lat = df_map["Lat"].mean()
    center_lon = df_map["Lon"].mean()

    # 3. Construction de la carte "couche par couche"
    fig = go.Figure()

    fig.add_trace(
        go.Scattermapbox(
            lat=df_map["Lat"],
            lon=df_map["Lon"],
            mode="markers+text",  # On exige les deux
            marker=dict(
                size=15,  # Gros point rouge
                color="red",
                opacity=1,
            ),
            text=df_map["Rang_Str"],  # Le numéro
            textposition="top center",  # JUSTE AU-DESSUS du point (plus sûr que 'middle center')
            textfont=dict(
                size=16, color="black", family="Arial Black"  # Texte noir bien gras
            ),
            hovertext=df_map["ADRESSE_CELLULE"],  # Infobulle
            hoverinfo="text",
        )
    )

    # 4. Configuration du fond de carte
    fig.update_layout(
        mapbox=dict(
            style="open-street-map",  # Fond clair
            zoom=10,
            center=dict(lat=center_lat, lon=center_lon),
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)


# --- Mise en page Colonnes ---
col_nuit, col_jour = st.columns(2)

# Définition Nuit (22h-06h) / Jour (06h-22h)
df_nuit = df_filtered[(df_filtered["HEURE"] >= 22) | (df_filtered["HEURE"] < 6)]
df_jour = df_filtered[(df_filtered["HEURE"] >= 6) & (df_filtered["HEURE"] < 22)]

with col_nuit:
    st.markdown("### 🛌 Nuit (22h-06h)")
    top_nuit = get_top_5_mapped(df_nuit)

    if not top_nuit.empty:
        # Affichage Tableau (Indexé par Rang pour correspondre à la carte)
        st.dataframe(
            top_nuit[["Rang", "ADRESSE_CELLULE", "Volume"]].set_index("Rang"),
            use_container_width=True,
        )
        # Affichage Carte
        display_top5_map(top_nuit)
    else:
        st.caption("Aucune donnée géolocalisée significative la nuit.")

with col_jour:
    st.markdown("### ☀️ Jour (06h-22h)")
    top_jour = get_top_5_mapped(df_jour)

    if not top_jour.empty:
        # Affichage Tableau
        st.dataframe(
            top_jour[["Rang", "ADRESSE_CELLULE", "Volume"]].set_index("Rang"),
            use_container_width=True,
        )
        # Affichage Carte
        display_top5_map(top_jour)
    else:
        st.caption("Aucune donnée géolocalisée significative le jour.")

st.divider()

# ==============================================================================
# 5. ANALYSE SPATIO-TEMPORELLE (SCATTER PLOT)
# ==============================================================================
st.subheader("📍 Chronologie des Déplacements (Villes)")
st.caption(
    "Visualisation des changements de ville au cours du temps (Axe X = Temps, Axe Y = Ville)"
)

if not df_filtered.empty:
    # Scatter Plot interactif
    fig_scatter = px.scatter(
        df_filtered,
        x="DATE_HEURE_UTC_FR",
        y="VILLE_CELLULE",
        color="DIRECTION",  # Différencier Entrant/Sortant
        hover_data=["ADRESSE_CELLULE", "NOM_CORRESPONDANT"],
        height=500,
        title="Déplacements par Ville",
    )
    # On relie les points par une ligne grise fine pour voir le cheminement
    fig_scatter.add_trace(
        go.Scatter(
            x=df_filtered["DATE_HEURE_UTC_FR"],
            y=df_filtered["VILLE_CELLULE"],
            mode="lines",
            line=dict(color="gray", width=0.5, dash="dot"),
            showlegend=False,
            hoverinfo="skip",
        )
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info("Pas de données pour la période sélectionnée.")

# ==============================================================================
# 7. CARTOGRAPHIE (Si données dispo)
# ==============================================================================
st.subheader("📍 Cartographie des Bornages")

# On ne garde que les points valides
df_map = df_filtered.dropna(subset=["LATITUDE", "LONGITUDE"])

if not df_map.empty:
    # Option 1 : Carte simple avec st.map (Points rouges)
    # st.map(df_map, latitude="LATITUDE", longitude="LONGITUDE")

    # Option 2 : Carte avancée avec Plotly (Couleur par Direction ou Type)
    # Cela permet d'avoir des tooltips au survol
    fig_map = px.scatter_mapbox(
        df_map,
        lat="LATITUDE",
        lon="LONGITUDE",
        color="DIRECTION",  # ou "VILLE_CELLULE"
        hover_name="ADRESSE_CELLULE",
        hover_data=["DATE_HEURE_UTC_FR", "MSISDN_CORRESPONDANT"],
        zoom=5,
        height=500,
    )
    fig_map.update_layout(mapbox_style="open-street-map")
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

else:
    st.info(
        "Aucune donnée GPS exploitable pour la cartographie sur la période sélectionnée."
    )

# ==============================================================================
# 8. TOP 10 VILLES & ADRESSES (GLOBAL)
# ==============================================================================
c1, c2 = st.columns(2)
with c1:
    st.markdown("##### 🏙️ Top 10 Villes")
    st.dataframe(
        df_filtered["VILLE_CELLULE"].value_counts().head(10), use_container_width=True
    )

with c2:
    st.markdown("##### 📡 Top 10 Relais (Adresses)")
    st.dataframe(
        df_filtered["ADRESSE_CELLULE"].value_counts().head(10), use_container_width=True
    )

# ==============================================================================
# 8. TABLEAU DE DONNÉES BRUTES (EXPORT)
# ==============================================================================
st.divider()
with st.expander("📂 Consulter les données brutes filtrées"):
    st.dataframe(
        df_filtered[
            [
                "DATE_HEURE_UTC_FR",
                "TYPE_COMMUNICATION",
                "DIRECTION",
                "MSISDN_CORRESPONDANT",
                "NOM_CORRESPONDANT",
                "DUREE_SECONDES",
                "ADRESSE_CELLULE",
                "VILLE_CELLULE",
            ]
        ],
        use_container_width=True,
    )

st.markdown("---")
# Bouton de retour
if st.button("⬅️ Retour à la sélection des données", use_container_width=True):
    st.switch_page("pages/Mes_Donnees.py")
