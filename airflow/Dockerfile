FROM astrocrpublic.azurecr.io/runtime:3.1-5

# Installation de dbt-snowflake dans l'environnement Airflow
RUN pip install dbt-snowflake

ENV AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager