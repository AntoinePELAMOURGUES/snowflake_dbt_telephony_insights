from .preprocess_functions import *

def preprocess_data(file1, file2, sheet_name=0):
    # Définition des colonnes attendues pour chaque fichier
    expected_columns_file1 = ["Type d'appel", "Abonné", "Correspondant", "Date", "Durée", "CIREF", "IMEI", "IMSI"]
    expected_columns_file2 = ["CIREF", "Adresse", "Comp. adresse", "Code postal", "Bureau Distributeur", "Coordonnée X", "Coordonnée Y"]

    # Lecture des fichiers Excel avec spécification des types de données
    df1 = pd.read_excel(file1, sheet_name=sheet_name, dtype={"Abonné": str, "Correspondant": str, "IMEI": str, "IMSI": str, "CIREF": str, "Durée": str, "Type d'appel" : str})
    df2 = pd.read_excel(file2, dtype= {"CIREF": str, "Adresse" : str, "Comp. adresse" : str, "Code postal" : str, "Bureau Distributeur" : str, "Coordonnée X": str, "Coordonnée Y": str})
    # Supprimer des dernières lignes contenant des instructions SRR
    df1 = df1.drop(df1.index[-1])
    df2 = df2.drop(df2.index[-1])
    # Filtrage des colonnes disponibles pour chaque DataFrame
    available_columns_1 = df1.columns.tolist()
    filtered_columns_1 = list(set(expected_columns_file1) & set(available_columns_1))
    available_columns_2 = df2.columns.tolist()
    filtered_columns_2 = list(set(expected_columns_file2) & set(available_columns_2))

    # Sélection des colonnes filtrées
    df1 = df1[filtered_columns_1]
    df2 = df2[filtered_columns_2]

    # Remplissage des valeurs manquantes dans la colonne 'Abonné'
    df1['Abonné'] = df1['Abonné'].ffill().bfill()

    # Fusion des DataFrames sur la colonne "CIREF"
    df = df1.merge(df2, on="CIREF", how="left")

    # Suppression des colonnes non nécessaires
    deleted_columns = ['Critère Recherché_x', 'Commentaire_x', '3ème interlocuteur', 'Nature Correspondant',
                       'Nature 3ème interlocuteur', 'GCI_x', 'EGCI_x', 'NGCI_x', 'Code PLMN',
                       'Volume de données montant', 'Volume de données descendant', "Opérateur d'itinérance", 'Indicateur RO', 'Décalage horaire',
                       'Service de Base', 'IPV4 VO Wifi', 'IPV6 VO Wifi',
                       'Port Source VO Wifi', 'Critère Recherché_y', 'Commentaire_y', 'GCI_y',
                       'EGCI_y', 'NGCI_y', 'Système', 'Nom du site', 'Code zone', 'Coordonnée Z', 'Début asso. CIREF/GCI', 'Fin asso. CIREF/GCI']
    df = df.drop(columns=[col for col in deleted_columns if col in df.columns])

    # Application des fonctions de prétraitement
    if 'Date' in df.columns:
        df = transform_date(df, 'Date')
    if 'Abonné' in df.columns:
        df = clean_cell_number(df, 'Abonné')
    if 'Correspondant' in df.columns:
        df = clean_cell_number(df, 'Correspondant')
    if "Type d'appel" in df.columns:
        df["Type d'appel"] = df["Type d'appel"].astype(str).str.upper().apply(reset_accent)
    if 'Bureau Distributeur' in df.columns:
        df = clean_city(df, columns='Bureau Distributeur')

    # Création d'une colonne d'adresse complète
    if 'Adresse' in df.columns and 'Code postal' in df.columns:
        df['Adresse'] = df['Adresse'] + " " + df['Code postal'] + " " + df['VILLE']
        df['Adresse'] = df['Adresse'].astype(str).str.upper().str.replace(r'\s+', ' ', regex=True).apply(reset_accent)
        df = df.drop(columns=['Comp. adresse', 'Code postal'])

    # Renommage des colonnes
    definitive_columns = ["Type d'appel", "Abonné", "Correspondant", "Date", "Durée", "CIREF", "IMEI", "IMSI", "Adresse", "Ville", 'Années', 'Mois', 'Heure', 'Jour de la semaine', "Coordonnée X", "Coordonnée Y"]
    final_columns = [reset_accent(col).upper() for col in definitive_columns]
    df = df.rename(columns=dict(zip(definitive_columns, final_columns)))
    return df