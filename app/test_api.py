import requests
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:8080"
USERNAME = "api_bot"
PASSWORD = "api_password_123"
DAG_ID = "telephony_dbt_transformation"

print("🔄 Étape 1 : Demande du Token...")

try:
    # 1. Auth
    auth_response = requests.post(
        f"{BASE_URL}/auth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"username": USERNAME, "password": PASSWORD},
    )

    if auth_response.status_code not in [200, 201]:
        print(f"❌ Échec Auth : {auth_response.status_code} - {auth_response.text}")
        exit()

    token = auth_response.json().get("access_token")
    print(f"✅ Token OK (Status {auth_response.status_code})")

    # 2. Trigger
    print("\n🚀 Étape 2 : Lancement du DAG...")

    # Note : Si vous utilisez l'API v2, l'URL contient /v2/
    dag_url = f"{BASE_URL}/api/v2/dags/{DAG_ID}/dagRuns"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # === LA CORRECTION EST ICI ===
    # On génère une date unique pour cette exécution
    logical_date = datetime.utcnow().isoformat() + "Z"

    payload = {
        "conf": {},
        "logical_date": logical_date,  # <--- Champ OBLIGATOIRE demandé par l'erreur 422
        "note": "Déclenché depuis script de test",
    }

    trigger_response = requests.post(dag_url, json=payload, headers=headers)

    print(f"📊 Status Code : {trigger_response.status_code}")
    print(f"📜 Réponse : {trigger_response.text}")

except Exception as e:
    print(f"💥 Erreur : {e}")
