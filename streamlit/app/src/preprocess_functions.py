import pandas as pd
import numpy as np
import re


def transform_date(df, columns: str):
    df[columns] = pd.to_datetime(df[columns])
    # Extraire l'année, le mois et le jour de la semaine
    df['Années'] = df[columns].dt.year
    df['Mois'] = df[columns].dt.month
    df['Heure'] = df[columns].dt.hour
    df['Jour de la semaine'] = df[columns].dt.day_name()
    # Mapper les jours de la semaine en français
    jours_semaine_fr = {
        'Monday': 'LUNDI',
        'Tuesday': 'MARDI',
        'Wednesday': 'MERCREDI',
        'Thursday': 'JEUDI',
        'Friday': 'VENDREDI',
        'Saturday': 'SAMEDI',
        'Sunday': 'DIMANCHE'
    }
    # Mapper les mois en français
    mois_fr = {
        1: 'JANVIER',
        2: 'FEVRIER',
        3: 'MARS',
        4: 'AVRIL',
        5: 'MAI',
        6: 'JUIN',
        7: 'JUILLET',
        8: 'AOUT',
        9: 'SEPTEMBRE',
        10: 'OCTOBRE',
        11: 'NOVEMBRE',
        12: 'DECEMBRE'
    }
    # Remplacer les numéros de mois par leur équivalent en français
    df['Mois'] = df['Mois'].map(mois_fr)
    # Remplacer les noms des jours par leur équivalent en français
    df['Jour de la semaine'] = df['Jour de la semaine'].map(jours_semaine_fr)
    return df

def clean_cell_number(df, columns:str):
    df[columns] = df[columns].astype(str)
    df[columns] = df[columns].str.split(',').str[0]
    df[columns] = df[columns].replace(r'^0693', '262693', regex=True)
    df[columns] = df[columns].replace(r'^0692', '262692', regex=True)
    df[columns] = df[columns].replace(r'^06', '336', regex=True)
    df[columns] = df[columns].replace(r'^07', '337', regex=True)
    df[columns] = df[columns].replace(r'^02', '2622', regex=True)
    return df

def reset_accent(chaine):
    accent_fr = {
    'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
    'à': 'a', 'â': 'a', 'ä': 'a',
    'ç': 'c',
    'î': 'i', 'ï': 'i',
    'ô': 'o', 'ö': 'o',
    'ù': 'u', 'û': 'u', 'ü': 'u',
    'ÿ': 'y',
    # Ajout des majuscules
    'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
    'À': 'A', 'Â': 'A', 'Ä': 'A',
    'Ç': 'C',
    'Î': 'I', 'Ï': 'I',
    'Ô': 'O', 'Ö': 'O',
    'Ù': 'U', 'Û': 'U', 'Ü': 'U'
}
    for accent, sans_accent in accent_fr.items():
        chaine = chaine.replace(accent, sans_accent)
    return chaine


def clean_city(df, columns: str):
    df[columns] = df[columns].astype(str)
    df = df.rename(columns={columns: 'VILLE'})
    df['VILLE']= df['VILLE'].str.upper()
    df['VILLE']= df['VILLE'].str.replace("-", " ")
    df['VILLE']= df['VILLE'].str.replace("SAINT", "ST")
    df['VILLE']= df['VILLE'].str.replace("SAINTE", "STE")
    df['VILLE']= df['VILLE'].str.replace("L'", "")
    df['VILLE'] = df['VILLE'].apply(reset_accent)
    df['VILLE'] = df['VILLE'].str.replace(r'\s+', ' ', regex=True).str.strip()
    return df


def convert_date(date_str):
    # Extraire le fuseau horaire avec une expression régulière
    match = re.search(r'UTC([+-]\d)', date_str)
    if match:
        utc_offset = int(match.group(1))  # Récupérer le décalage horaire
    else:
        utc_offset = 0  # Valeur par défaut si aucun fuseau horaire n'est trouvé
    # Retirer le fuseau horaire et convertir en datetime
    date_without_tz = date_str.split(' UTC')[0]  # Retirer ' UTC+X'
    dt = pd.to_datetime(date_without_tz, format='%d/%m/%Y - %H:%M:%S')
    # Ajuster le fuseau horaire à UTC+4
    dt = dt + pd.Timedelta(hours=(4 - utc_offset))  # Ajustement basé sur l'offset
    return dt

# Fonction pour nettoyer les chaînes
def clean_number(number_str):
    # Utiliser une expression régulière pour supprimer le préfixe et le suffixe
    return re.sub(r'^\=\("\s*|\s*"\)$', '', number_str)

def replace_unknown_ville(row):
    reunion_postal_codes = {
    "97400": "ST DENIS",
    "97410": "ST PIERRE",
    "97411": "BOIS DE NEFLES",
    "97412": "BRAS PANON",
    "97413": "CILAOS",
    "97414": "ENTRE DEUX",
    "97416": "LA CHALOUPE",
    "97417": "ST BERNARD",
    "97418": "LA PLAINE DES CAFRES",
    "97419": "LA POSSESSION",
    "97420": "LE PORT",
    "97421": "LA RIVIERE ST LOUIS",
    "97422": "LA SALINE",
    "97423": "LE GUILLAUME",
    "97424": "PITON ST LEU",
    "97425": "LES AVRIONS",
    "97426": "LES TROIS BASSINS",
    "97427": "L'ETANG SALE",
    "97428": "LA NOUVELLE",
    "97429": "PETITE ILE",
    "97430": "LE TAMPON",
    "97431": "LA PLAINE DES PALMISTES",
    "97432": "LA RAVINE DES CABRIS",
    "97433": "SALAZIE - HELL BOURG",
    "97434": "ST GILLES LES BAINS",
    "97435": "BERNICA",
    "97436": "ST LEU",
    "97437": "STE ANNE",
    "97438": "STE MARIE",
    "97439": "STE ROSE",
    "97440": "ST ANDRE",
    "97441": "STE SUZANNE",
    "97442": "BASSE VALLEE",
    "97450": "ST LOUIS",
    "97460": "ST PAUL",
    "97470": "ST BENOIT"
}
    if row['VILLE'] == 'ville inconnue':
        return reunion_postal_codes.get(row['CODE POSTAL'], 'ville inconnue')  # Remplace par la ville correspondante ou garde 'ville inconnue'
    return row['VILLE']


def extract_city(address):
    if isinstance(address, str):  # Vérifie si l'adresse est une chaîne
        # Regex pour capturer la ville après le code postal (5 chiffres)
        match = re.search(r'\d{5}\s+(.*)', address)
        if match:
            return match.group(1).strip()  # Retourne la ville sans espaces superflus
    return None  # Retourne None si aucune correspondance n'est trouvée ou si l'adresse n'est pas une chaîne

