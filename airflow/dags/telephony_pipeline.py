from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Définition du DAG
with DAG(
    dag_id="telephony_dbt_transformation",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # On ne veut pas qu'il tourne tout seul, mais sur déclenchement
    catchup=False,
    tags=["telephony", "dbt"],
) as dag:

    # Tâche 1 : Vérifier la connexion (Optionnel, bon pour le debug)
    debug_cmd = BashOperator(task_id="debug_dbt_version", bash_command="dbt --version")

    # Tâche 2 : Lancer les transformations avec SELECTEUR DYNAMIQUE
    run_dbt = BashOperator(
        task_id="dbt_run",
        bash_command="""
            # 1. Copie du projet vers /tmp (Zone inscriptible)
            rm -rf /tmp/dbt_project && \
            cp -r /usr/local/airflow/dags/dbt_project /tmp/dbt_project && \

            # 2. DÉPLACEMENT ET INSTALLATION DES DÉPENDANCES (C'est ici qu'il manquait des choses !)
            cd /tmp/dbt_project && \
            dbt deps && \

            # 3. Logique de sélection dynamique
            {% if dag_run.conf.get('dbt_selector', 'all') == 'all' %}
                echo "🚀 Lancement complet (Full Run)" && \
                dbt run --profiles-dir . --target dev
            {% else %}
                echo "🎯 Lancement ciblé pour le tag : {{ dag_run.conf['dbt_selector'] }}" && \
                # Le '+' à la fin signifie : "Lance ce modèle ET tout ce qui en dépend"
                dbt run --profiles-dir . --target dev --select tag:{{ dag_run.conf['dbt_selector'] }}+
            {% endif %}
        """,
        env={
            "DBT_ACCOUNT": "{{ conn.snowflake_conn.extra_dejson.account }}",
            "DBT_USER": "{{ conn.snowflake_conn.login }}",
            "DBT_PASSWORD": "{{ conn.snowflake_conn.password }}",
            "DBT_ROLE": "{{ conn.snowflake_conn.extra_dejson.role }}",
            "DBT_WAREHOUSE": "{{ conn.snowflake_conn.extra_dejson.warehouse }}",
            "DBT_DATABASE": "{{ conn.snowflake_conn.schema }}",
            "DBT_SCHEMA": "DBT_STAGING",
        },
    )

    debug_cmd >> run_dbt
