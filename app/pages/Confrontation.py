import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark import Session
from datetime import datetime  # <--- Import ajouté pour les filtres

# ==============================================================================
# 1. CONFIGURATION & ROUTEUR
# ==============================================================================
st.set_page_config(page_title="Confrontation", page_icon="⚔️", layout="wide")


@st.cache_resource
def create_snowpark_session():
    try:
        return Session.builder.configs(st.secrets["snowflake"]).create()
    except Exception as e:
        st.error(f"Erreur connexion Snowflake: {e}")
        return None


session = create_snowpark_session()

# Vérification du contexte
if (
    "analysis_context" not in st.session_state
    or not st.session_state["analysis_context"]
):
    st.warning("Aucun élément sélectionné pour la confrontation.")
    if st.button("Retourner aux Dossiers"):
        st.switch_page("pages/Gestion_Dossiers.py")
    st.stop()

# Récupération des fichiers sélectionnés
selected_files = st.session_state["analysis_context"]
filenames = [f["FILENAME"] for f in selected_files]
# On suppose qu'on travaille dans le même dossier pour l'instant
dossier_id = selected_files[0]["DOSSIER_ID"]

# DÉTECTION DU MODE (Cibles vs Zones)
types_fichiers = set(f["FILE_TYPE"] for f in selected_files)
has_mt = any("MT" in t for t in types_fichiers)
has_href = any("HREF" in t for t in types_fichiers)


# ==============================================================================
# MODE 1 : CONFRONTATION DE CIBLES (MT20 / MT24)
# ==============================================================================
def run_confrontation_cibles():
    st.title(f"⚔️ Confrontation de Cibles ({len(filenames)} éléments)")

    # 1. CHARGEMENT DONNÉES
    with st.spinner("Chargement et croisement des communications..."):
        query = """
            SELECT
                SOURCE_FILENAME,
                DATE_HEURE_UTC_FR,
                MSISDN_CORRESPONDANT,
                NOM_CORRESPONDANT,
                VILLE_CELLULE,
                ADRESSE_CELLULE,
                LATITUDE,
                LONGITUDE,
                MSISDN_CIBLE,
                IMEI_CIBLE
            FROM MARTS.PROD.FCT_COMMUNICATIONS
            WHERE DOSSIER_ID = ?
            AND SOURCE_FILENAME IN ({})
        """.format(
            ",".join([f"'{f}'" for f in filenames])
        )

        df = session.sql(query, params=[dossier_id]).to_pandas()
        df["DATE_HEURE_UTC_FR"] = pd.to_datetime(df["DATE_HEURE_UTC_FR"])

        # Préparation pour les filtres
        df["DATE"] = df["DATE_HEURE_UTC_FR"].dt.date
        df["HEURE"] = df["DATE_HEURE_UTC_FR"].dt.hour
        df["HEURE_SIMPLE"] = df["DATE_HEURE_UTC_FR"].dt.strftime("%Y-%m-%d %H:00")

    # 2. FILTRES (Sidebar)
    with st.sidebar:
        st.header("🛠️ Filtres Confrontation")

        # Filtre Date
        min_ts = df["DATE"].min()
        max_ts = df["DATE"].max()
        if pd.isnull(min_ts):
            min_ts = datetime.today().date()
        if pd.isnull(max_ts):
            max_ts = datetime.today().date()

        date_range = st.date_input("📅 Période", [min_ts, max_ts])

        # Filtre Heure
        hour_range = st.slider("⏰ Créneau Horaire", 0, 23, (0, 23))

        # Application des filtres
        if len(date_range) == 2:
            mask_date = (df["DATE"] >= date_range[0]) & (df["DATE"] <= date_range[1])
        else:
            mask_date = df["DATE"] == date_range[0]

        mask_hour = (df["HEURE"] >= hour_range[0]) & (df["HEURE"] <= hour_range[1])

        # DATAFRAME FILTRÉ (C'est celui qu'on utilisera partout après)
        df_filtered = df[mask_date & mask_hour]

        st.divider()
        st.metric("Volume analysé", f"{len(df_filtered)} communications")

    # --- A. CORRESPONDANTS COMMUNS ---
    st.subheader("🤝 Correspondants Communs")

    # On cherche les numéros contactés par PLUSIEURS fichiers sources différents
    # Note : on utilise df_filtered ici
    common_contacts = (
        df_filtered.groupby(["MSISDN_CORRESPONDANT", "NOM_CORRESPONDANT"])[
            "SOURCE_FILENAME"
        ]
        .nunique()
        .reset_index()
    )
    # On garde ceux qui apparaissent dans au moins 2 fichiers différents
    common_contacts = common_contacts[
        common_contacts["SOURCE_FILENAME"] > 1
    ].sort_values("SOURCE_FILENAME", ascending=False)

    if not common_contacts.empty:
        st.success(
            f"{len(common_contacts)} correspondants communs identifiés sur la période."
        )

        # TABLEAU DE SYNTHÈSE (Avant le détail)
        st.write("##### Liste des correspondants communs :")
        st.dataframe(
            common_contacts.rename(
                columns={
                    "MSISDN_CORRESPONDANT": "Numéro",
                    "NOM_CORRESPONDANT": "Identité (Annuaire)",
                    "SOURCE_FILENAME": "Nb Cibles en contact",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.divider()

        # Détail Interactif
        selected_contact = st.selectbox(
            "🔎 Analyser le détail des échanges pour :",
            common_contacts["MSISDN_CORRESPONDANT"]
            + " - "
            + common_contacts["NOM_CORRESPONDANT"],
        )

        if selected_contact:
            msisdn_filter = selected_contact.split(" - ")[0]
            # On filtre sur le DataFrame principal filtré
            detail_communs = df_filtered[
                df_filtered["MSISDN_CORRESPONDANT"] == msisdn_filter
            ]

            # Tableau croisé dynamique (Qui a appelé quand ?)
            pivot_table = detail_communs.pivot_table(
                index="SOURCE_FILENAME", values="DATE_HEURE_UTC_FR", aggfunc="count"
            ).rename(columns={"DATE_HEURE_UTC_FR": "Nb Interactions"})

            st.dataframe(pivot_table, use_container_width=True)

    else:
        st.info("Aucun correspondant commun trouvé sur la période sélectionnée.")

    st.divider()

    # --- B. PROXIMITÉ GÉOGRAPHIQUE (CO-PRÉSENCE) ---
    st.subheader("📍 Coïncidences Spatio-Temporelles")
    st.caption(
        "Recherche de moments où les cibles ont borné dans la **même ville** au cours de la **même heure**."
    )

    # On utilise df_filtered ici aussi pour respecter le créneau horaire choisi
    df_geo = df_filtered.dropna(subset=["VILLE_CELLULE"])

    rencontres = (
        df_geo.groupby(["HEURE_SIMPLE", "VILLE_CELLULE"])["SOURCE_FILENAME"]
        .nunique()
        .reset_index()
    )
    rencontres_possibles = rencontres[rencontres["SOURCE_FILENAME"] > 1].sort_values(
        "HEURE_SIMPLE", ascending=False
    )

    if not rencontres_possibles.empty:
        st.warning(f"⚠️ {len(rencontres_possibles)} créneaux de proximité détectés !")

        col_table, col_map = st.columns([1, 1])

        with col_table:
            st.dataframe(
                rencontres_possibles.rename(
                    columns={
                        "HEURE_SIMPLE": "Créneau (1h)",
                        "VILLE_CELLULE": "Lieu",
                        "SOURCE_FILENAME": "Nb Cibles Présentes",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )

        with col_map:
            # Merge pour récupérer les coordonnées
            df_map_hits = pd.merge(
                df_geo,
                rencontres_possibles[["HEURE_SIMPLE", "VILLE_CELLULE"]],
                on=["HEURE_SIMPLE", "VILLE_CELLULE"],
            )

            if not df_map_hits.empty and "LATITUDE" in df_map_hits.columns:
                # Nettoyage GPS pour la carte
                df_map_hits["LATITUDE"] = pd.to_numeric(
                    df_map_hits["LATITUDE"], errors="coerce"
                )
                df_map_hits["LONGITUDE"] = pd.to_numeric(
                    df_map_hits["LONGITUDE"], errors="coerce"
                )
                df_map_hits = df_map_hits.dropna(subset=["LATITUDE", "LONGITUDE"])

                if not df_map_hits.empty:
                    fig = px.scatter_mapbox(
                        df_map_hits,
                        lat="LATITUDE",
                        lon="LONGITUDE",
                        color="SOURCE_FILENAME",
                        hover_data=["DATE_HEURE_UTC_FR", "ADRESSE_CELLULE"],
                        zoom=9,
                        height=400,
                        title="Lieux de rencontre potentiels",
                    )
                    fig.update_layout(
                        mapbox_style="carto-positron",
                        margin={"r": 0, "t": 0, "l": 0, "b": 0},
                    )
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Aucune proximité géographique détectée sur cette période.")


# ==============================================================================
# MODE 2 : CONFRONTATION DE ZONES (HREF)
# ==============================================================================
def run_confrontation_zones():
    st.title(f"📡 Analyse Croisée de Zones (HREF)")

    zones_info = {f["FILENAME"]: f["TARGET_IDENTIFIER"] for f in selected_files}
    st.write(
        f"**Zones confrontées :** {', '.join([f'Zone {v}' for v in zones_info.values()])}"
    )

    # 1. CHARGEMENT
    with st.spinner("Calcul des intersections..."):
        query = """
            SELECT
                OPERATEUR,
                NUMERO_ZONE,
                IMSI,
                IMEI,
                MSISDN,
                DATE_HEURE_UTC_FR,
                DESCRIPTION_EVENT,
                VILLE_CELLULE,
                ADRESSE_CELLULE,
                LATITUDE,
                LONGITUDE,
                SOURCE_FILENAME
            FROM MARTS.PROD.FCT_BORNAGE_ZONES
            WHERE DOSSIER_ID = ?
            AND SOURCE_FILENAME IN ({})
            AND IMSI IS NOT NULL
        """.format(
            ",".join([f"'{f}'" for f in filenames])
        )

        df = session.sql(query, params=[dossier_id]).to_pandas()

    if df.empty:
        st.error("Aucune donnée trouvée.")
        return

    # 2. CALCUL DES INTERSECTIONS
    ranking = df.groupby("IMSI")["NUMERO_ZONE"].nunique().reset_index()
    ranking.columns = ["IMSI", "Nb_Zones_Frequentees"]

    top_suspects = ranking[ranking["Nb_Zones_Frequentees"] > 1].sort_values(
        "Nb_Zones_Frequentees", ascending=False
    )

    nb_total_imsis = df["IMSI"].nunique()
    nb_suspects = len(top_suspects)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total IMSI captés", nb_total_imsis)
    c2.metric("IMSI en intersection", nb_suspects)
    c3.metric("Taux de pertinence", f"{(nb_suspects/nb_total_imsis)*100:.2f}%")

    st.divider()

    if not top_suspects.empty:
        st.subheader("🏆 Classement des IMSI Communs")

        top_suspects["Label"] = (
            top_suspects["IMSI"]
            + " ("
            + top_suspects["Nb_Zones_Frequentees"].astype(str)
            + " zones)"
        )

        col_list, col_details = st.columns([1, 2])

        with col_list:
            selected_label = st.radio(
                "Sélectionnez un IMSI :", top_suspects["Label"], index=0
            )
            selected_imsi = selected_label.split(" ")[0]

        with col_details:
            st.markdown(f"#### 🔎 Détail pour l'IMSI : `{selected_imsi}`")

            details_imsi = df[df["IMSI"] == selected_imsi].sort_values(
                "DATE_HEURE_UTC_FR"
            )

            st.dataframe(
                details_imsi[
                    [
                        "NUMERO_ZONE",
                        "DATE_HEURE_UTC_FR",
                        "DESCRIPTION_EVENT",
                        "VILLE_CELLULE",
                        "IMEI",
                        "MSISDN",
                    ]
                ],
                column_config={
                    "NUMERO_ZONE": "Zone",
                    "DATE_HEURE_UTC_FR": st.column_config.DatetimeColumn(
                        "Date/Heure", format="DD/MM HH:mm:ss"
                    ),
                    "DESCRIPTION_EVENT": "Événement",
                },
                use_container_width=True,
                hide_index=True,
            )

            fig_timeline = px.scatter(
                details_imsi,
                x="DATE_HEURE_UTC_FR",
                y="NUMERO_ZONE",
                color="NUMERO_ZONE",
                title="Chronologie des apparitions par Zone",
            )
            st.plotly_chart(fig_timeline, use_container_width=True, key="timeline_zone")

    else:
        st.info("Aucun IMSI commun trouvé entre ces zones.")


# ==============================================================================
# LANCEMENT (MAIN)
# ==============================================================================
if has_href and has_mt:
    st.error(
        "⚠️ Mode Mixte non supporté : Veuillez sélectionner soit uniquement des Cibles (MT), soit uniquement des Zones (HREF)."
    )
elif has_href:
    run_confrontation_zones()
else:
    run_confrontation_cibles()

st.markdown("---")
if st.button("⬅️ Retour à la sélection des données", use_container_width=True):
    st.switch_page("pages/Mes_Donnees.py")
