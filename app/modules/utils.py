import re


def validate_password_strength(password):
    """
    Valide que le mot de passe respecte les critères de l'ANSSI.
    Retourne (True, "") si OK, sinon (False, "Message d'erreur").
    """
    # 1. Vérification de la longueur
    if len(password) < 12:
        return False, "Le mot de passe doit contenir au moins 12 caractères."

    # 2. Vérification Majuscule
    if not re.search(r"[A-Z]", password):
        return False, "Le mot de passe doit contenir au moins une majuscule."

    # 3. Vérification Minuscule
    if not re.search(r"[a-z]", password):
        return False, "Le mot de passe doit contenir au moins une minuscule."

    # 4. Vérification Chiffre
    if not re.search(r"\d", password):
        return False, "Le mot de passe doit contenir au moins un chiffre."

    # 5. Vérification Caractère Spécial
    if not re.search(r"[ !@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]", password):
        return False, "Le mot de passe doit contenir au moins un caractère spécial."

    return True, ""


import requests
import streamlit as st
from datetime import datetime  # <--- NE PAS OUBLIER


def trigger_airflow_pipeline(target_tag="all"):
    """
    Déclenche le DAG Airflow avec un tag spécifique pour dbt"""
    # Configuration
    AIRFLOW_HOST = "http://localhost:8080"
    USERNAME = "admin"
    PASSWORD = "admin"
    DAG_ID = "telephony_dbt_transformation"

    try:
        # --- ÉTAPE 1 : Récupérer le Token ---
        auth_url = f"{AIRFLOW_HOST}/auth/token"
        auth_payload = {"username": USERNAME, "password": PASSWORD}
        auth_headers = {"Content-Type": "application/x-www-form-urlencoded"}

        token_response = requests.post(
            auth_url, data=auth_payload, headers=auth_headers
        )

        if token_response.status_code not in [200, 201]:
            st.error(
                f"🔐 Erreur d'authentification Airflow ({token_response.status_code})"
            )
            return

        token = token_response.json().get("access_token")

        # --- ÉTAPE 2 : Déclencher le DAG ---
        # Si vous utilisez l'API v2, changez v1 par v2 ici
        trigger_url = f"{AIRFLOW_HOST}/api/v2/dags/{DAG_ID}/dagRuns"

        trigger_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Payload complet pour éviter l'erreur 422
        payload = {
            "conf": {"dbt_selector": target_tag},
            "logical_date": datetime.utcnow().isoformat() + "Z",  # Indispensable
            "note": f"Déclenché par {st.session_state.get('user_email', 'Utilisateur Streamlit')}",
        }

        response = requests.post(trigger_url, json=payload, headers=trigger_headers)

        if response.status_code in [200, 201]:
            st.success("✅ Pipeline Airflow déclenché avec succès !")
            st.toast("Le traitement des données a commencé.")
        else:
            st.error(f"❌ Erreur Airflow ({response.status_code}) : {response.text}")

    except Exception as e:
        st.error(f"❌ Impossible de contacter Airflow : {str(e)}")
