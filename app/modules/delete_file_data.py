import streamlit as st


# Ajout de 'session' en premier argument 👇
def delete_file_data(session, file_id, file_type, filename, dossier_id):
    """Nettoie les données Raw et le Log pour un fichier donné."""
    try:
        # 1. Identification des tables RAW à nettoyer
        tables_to_clean = []

        if file_type == "MT20":
            tables_to_clean.append("RAW_DATA.PNIJ_SRC.RAW_MT20")
        elif file_type == "MT24":
            tables_to_clean.append("RAW_DATA.PNIJ_SRC.RAW_MT24")
        elif file_type == "ANNUAIRE":
            tables_to_clean.append("RAW_DATA.PNIJ_SRC.RAW_ANNUAIRE")
        elif "HREF" in file_type or "Zone" in file_type:
            tables_to_clean = [
                "RAW_DATA.PNIJ_SRC.RAW_HREF_EVENTS_ORANGE",
                "RAW_DATA.PNIJ_SRC.RAW_HREF_CELLS_ORANGE",
                "RAW_DATA.PNIJ_SRC.RAW_HREF_SFR",
                "RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES",
            ]

        # 2. Exécution des suppressions RAW
        for table in tables_to_clean:
            # On utilise l'objet session passé en argument
            query = f"DELETE FROM {table} WHERE DOSSIER_ID = '{dossier_id}' AND SOURCE_FILENAME = '{filename}'"
            session.sql(query).collect()

        # 3. Suppression du Log (Métadonnée)
        query_log = (
            f"DELETE FROM DOSSIERS_DB.PROD.FILES_LOG WHERE FILE_ID = '{file_id}'"
        )
        session.sql(query_log).collect()

        return True
    except Exception as e:
        st.error(f"Erreur technique lors de la suppression : {e}")
        return False
