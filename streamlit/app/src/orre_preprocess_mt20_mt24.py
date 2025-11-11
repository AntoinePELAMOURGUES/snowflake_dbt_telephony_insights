import pandas as pd
from .preprocess_functions import *

def preprocess_data(file1):
    # Liste des colonnes attendues
    expected_columns = [
        "Date de début d'appel",
        "MSISDN Abonné",
        "Correspondant",
        "Type de communication",
        "Durée / Nbr SMS",
        "Adresse du relais",
        "IMEI abonné",
        "IMSI abonné"
    ]
    # Lire à nouveau avec usecols
    df = pd.read_csv(file1, header=1, sep=';', encoding='latin1', dtype={"MSISDN Abonné": str, "Correspondant": str, "IMEI abonné": str, "IMSI abonné": str, "Durée / Nbr SMS": str})
    available_columns = df.columns.tolist()
    # Filtrer les colonnes attendues qui sont disponibles
    filtered_columns = list(set(expected_columns) & set(available_columns))
    df = df[filtered_columns]
    rename_dict = {
        "Date de début d'appel": "DATE",
        "MSISDN Abonné": "ABONNE",
        "Type de communication": "TYPE D'APPEL",
        "Durée / Nbr SMS": "DUREE",
        "Adresse du relais": "ADRESSE",
        "IMEI abonné": "IMEI",
        "IMSI abonné": "IMSI",
        "Correspondant": "CORRESPONDANT"
    }
    # Renommer uniquement les colonnes présentes dans le DataFrame
    df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
    if 'DATE' in df.columns:
        df = transform_date(df, 'DATE')
    if "TYPE D'APPEL" in df.columns:
        df["TYPE D'APPEL"] = df["TYPE D'APPEL"].astype(str)
        df["TYPE D'APPEL"] = df["TYPE D'APPEL"].str.upper()
        df["TYPE D'APPEL"] = df["TYPE D'APPEL"].apply(reset_accent)
    if 'IMEI' in df.columns:
        df['IMEI'] = df['IMEI'].fillna("INDETERMINE")
        df['IMEI'] = df['IMEI'].astype(str)
        df["IMEI"] = df["IMEI"].apply(clean_number)
    if 'IMSI' in df.columns:
        df['IMSI'] = df['IMSI'].fillna("INDETERMINE")
        df["IMSI"] = df["IMSI"].astype('str')
        df['IMSI'] = df['IMSI'].apply(clean_number)
    if 'ABONNE' in df.columns:
        df['ABONNE'] = df['ABONNE'].fillna('')
        df['ABONNE'] = df['ABONNE'].apply(clean_number)
        df = clean_cell_number(df, 'ABONNE')
    if 'CORRESPONDANT' in df.columns:
        df['CORRESPONDANT'] = df['CORRESPONDANT'].fillna("DATA")
        df['CORRESPONDANT'] = df['CORRESPONDANT'].astype(str)
        df['CORRESPONDANT'] = df['CORRESPONDANT'].apply(clean_number)
        df = clean_cell_number(df, 'CORRESPONDANT')
    if 'ADRESSE' in df.columns:
        df['ADRESSE'] = df['ADRESSE'].fillna("INDETERMINE")
        df['ADRESSE'] = df['ADRESSE'].astype(str)
        df['ADRESSE'] = df['ADRESSE'].str.upper()
        df['ADRESSE'] = df['ADRESSE'].apply(reset_accent)
        # Appliquer la fonction pour créer une nouvelle colonne 'Ville'
        df['VILLE'] = df['ADRESSE'].apply(extract_city)
        df = clean_city(df, columns='VILLE')
    df.fillna("INDETERMINE", inplace=True)
    final_rename_dict = {
        "Année": "ANNEE", "Mois": "MOIS", "Heure": "HEURE", "Jour de la semaine": "JOUR DE LA SEMAINE"}
    df.rename(columns={k: v for k, v in final_rename_dict.items() if k in df.columns}, inplace=True)

    return df