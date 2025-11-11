# **Synthèse des opérations sur les datasets**

## **Fichier: orre_preprocess_mt20_mt24.py**

### **Importation des bibliothèques :**
- **pandas** pour la manipulation des données.
- **numpy** pour les opérations mathématiques.
- **re** pour les expressions régulières.
- **streamlit** pour la création d'applications web interactives.

### **Fonction extract_city :**
- Extrait le nom de la ville à partir d'une chaîne de caractères représentant une adresse, en utilisant une expression régulière pour capturer la ville après le code postal à 5 chiffres.

### **Fonction preprocess_data :**
- Lit un fichier CSV avec des colonnes spécifiques en utilisant `pd.read_csv`, avec des paramètres pour le séparateur, l'encodage et les types de données.
- Filtre les colonnes du DataFrame pour ne conserver que celles attendues.
- Renomme certaines colonnes pour les rendre plus compréhensibles.
- Effectue des transformations sur la colonne Date si elle est présente :
  - Convertit les valeurs en type datetime.
  - Extrait l'année, le mois et le jour de la semaine.
  - Remplace les numéros de mois et les noms des jours par leurs équivalents en français.
- Remplace les valeurs manquantes dans les colonnes IMEI et IMSI par "Indéterminé".
- Effectue des remplacements sur les colonnes Abonné et Correspondant :
  - Remplace les préfixes de numéros de téléphone par d'autres valeurs spécifiques.
  - Remplit les valeurs manquantes avec "Data".
- Applique des transformations sur la colonne Adresse :
  - Convertit les valeurs en majuscules.
  - Extrait la ville en utilisant la fonction extract_city et applique des transformations pour standardiser les noms de ville.

---

## **Fichier: srr_preprocess_mt20_mt24.py**

### **Importation des bibliothèques :**
- **pandas** pour la manipulation des données.
- **numpy** pour les opérations mathématiques.

### **Fonction preprocess_data :**
- Lit deux fichiers Excel avec des colonnes spécifiques en utilisant `pd.read_excel`, avec des paramètres pour les types de données.
- Filtre les colonnes du DataFrame pour ne conserver que celles attendues.
- Remplit les valeurs manquantes dans la colonne Abonné par les valeurs précédentes ou suivantes.
- Fusionne les deux DataFrames sur la colonne CIREF.
- Supprime les colonnes spécifiées dans `deleted_columns`.
- Effectue des transformations sur la colonne Date si elle est présente :
  - Convertit les valeurs en type datetime.
  - Extrait l'année, le mois et le jour de la semaine.
  - Remplace les numéros de mois et les noms des jours par leurs équivalents en français.
- Effectue des remplacements sur les colonnes Abonné et Correspondant :
  - Remplace les préfixes de numéros de téléphone par d'autres valeurs spécifiques.
  - Remplit les valeurs manquantes avec "Data".
  - Sépare les valeurs multiples dans Correspondant et ne garde que la première.
- Renomme la colonne Bureau Distributeur en Ville et standardise les noms de ville.
- Crée une nouvelle colonne adresse_complete en combinant Adresse, Comp. adresse et Code postal, puis supprime les colonnes d'origine et renomme adresse_complete en Adresse.

---

## **Fichier: tcoi_preprocess_mt20_mt24.py**

### **Importation des bibliothèques :**
- **pandas** pour la manipulation des données.
- **numpy** pour les opérations mathématiques.
- **re** pour les expressions régulières.

### **Fonction convert_date :**
- Convertit une chaîne de date avec fuseau horaire en datetime.
- Ajuste le fuseau horaire à UTC+4.

### **Fonction clean_number :**
- Nettoie les chaînes en supprimant les préfixes et suffixes spécifiques.

### **Dictionnaire reunion_postal_codes :**
- Contient les codes postaux et les villes correspondantes de La Réunion.

### **Fonction replace_unknown_ville :**
- Remplace les villes inconnues par les villes correspondantes à leur code postal.

### **Fonction preprocess_data :**
- Lit un fichier CSV avec des colonnes spécifiques en utilisant `pd.read_csv`, avec des paramètres pour les types de données.
- Convertit les dates en utilisant `convert_date`.
- Extrait l'année, le mois et le jour de la semaine, puis remplace les noms de jours et mois par leurs équivalents en français.
- Nettoie et remplace les valeurs dans les colonnes CORRESPONDANT, DUREE, IMEI, IMSI, CIBLE, VILLE et ADRESSE2.
- Crée une colonne Adresse en combinant ADRESSE2, CODE POSTAL et VILLE.
- Supprime les colonnes spécifiées dans `deleted_columns`.
- Renomme certaines colonnes selon un dictionnaire de correspondance.
