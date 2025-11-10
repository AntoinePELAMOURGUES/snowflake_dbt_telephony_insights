import pandas as pd
import numpy as np
import re
from .preprocess_functions import *


def preprocess_data(file1):
    df = pd.read_csv(file1, sep=';', encoding='latin1', dtype={"CIBLE": str, "CORRESPONDANT": str, "DUREE": str, "IMSI": str, "IMEI": str, "CODE POSTAL": str,  "X": str, "Y": str, "VILLE": str, "ADRESSE2": str, "ADRESSE3": str, "ADRESSE4": str, "ADRESSE5": str})
    # Appliquer la fonction pour convertir les dates
    if 'DATE' in df.columns:
        df['DATE'] = df['DATE'].apply(convert_date)
        df = transform_date(df, 'DATE')
    if 'CORRESPONDANT' in df.columns:
        df['CORRESPONDANT'] = df['CORRESPONDANT'].fillna("DATA")
        df['CORRESPONDANT'] = df['CORRESPONDANT'].astype(str)
        df['CORRESPONDANT'] = df['CORRESPONDANT'].apply(clean_number)
        df = clean_cell_number(df, 'CORRESPONDANT')
    if 'DUREE' in df.columns:
        df["DUREE"] = df["DUREE"].fillna("0")
    if 'DIRECTION' in df.columns:
        df['DIRECTION'] = df['DIRECTION'].fillna("INDETERMINE")
        df['DIRECTION'] = df['DIRECTION'].str.upper()
    if 'IMEI' in df.columns:
        df['IMEI'] = df['IMEI'].fillna("INDETERMINE")
        df["IMEI"] = df["IMEI"].apply(clean_number)
    if 'IMSI' in df.columns:
        df["IMSI"] = df["IMSI"].astype('str')
        df['IMSI'] = df['IMSI'].apply(clean_number)
    if 'CIBLE' in df.columns:
        df['CIBLE'] = df['CIBLE'].apply(clean_number)
        df = clean_cell_number(df, 'CIBLE')
    if "TYPE" in df.columns:
        df["TYPE"] = df["TYPE"].astype(str)
        df["TYPE"] = df["TYPE"].str.upper()
        df["TYPE"] = df["TYPE"].apply(reset_accent)
    if 'VILLE' in df.columns:
        df['VILLE'] = df['VILLE'].fillna('INDETERMINE')
        df['VILLE'] = df.apply(replace_unknown_ville, axis=1)
        df = clean_city(df, columns='VILLE')
    if 'ADRESSE2' in df.columns:
        df['ADRESSE2'] = df['ADRESSE2'].fillna("INDETERMINE")
        df['CODE POSTAL'] = df['CODE POSTAL'].fillna("INDETERMINE")
     # Créer la colonne Adresse
    df["ADRESSE"] = np.where(
        df["ADRESSE2"] != "INDETERMINE",
        df["ADRESSE2"] + " " + df["CODE POSTAL"] + " " + df["VILLE"],
        'INDETERMINE'
    )
    # Nettoyer la colonne ADRESSE
    df['ADRESSE'] = df['ADRESSE'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df['ADRESSE'] = df['ADRESSE'].str.upper()
    df['ADRESSE'] = df['ADRESSE'].apply(reset_accent)
    deleted_columns = ['TYPE CORRESPONDANT', 'COMP.', 'EFFICACITE' , 'CELLID', 'ADRESSE IP VO WIFI', 'PORT SOURCE VO WIFI', 'ADRESSE2','ADRESSE3','ADRESSE4', 'ADRESSE5', 'PAYS', 'TYPE-COORD', 'CODE POSTAL']
    df.drop(columns=deleted_columns, inplace=True, errors='ignore')
    rename_dict = {"TYPE": "TYPE D'APPEL", "CIBLE": "ABONNE", "X": "LATITUDE", "Y": "LONGITUDE", "converted_date": "DATE", "Années": "ANNEE", "Mois": "MOIS", "Heure": "HEURE", "Jour de la semaine": "JOUR DE LA SEMAINE"
    }
    df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
    df.fillna("INDETERMINE", inplace=True)
    return df