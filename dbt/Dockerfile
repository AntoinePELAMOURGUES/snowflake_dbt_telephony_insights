# On utilise la même base performante que pour Streamlit
FROM ghcr.io/astral-sh/uv:python3.9-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /dbt

# Installation directe de l'adaptateur Snowflake (qui installe dbt-core aussi)
# On n'a pas besoin de requirements.txt complexe pour dbt, une ligne suffit.
RUN uv venv /dbt/.venv && \
    uv pip install --no-cache dbt-snowflake

# --- Image Finale ---
FROM python:3.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/dbt/.venv/bin:$PATH" \
    # Astuce : On dit à dbt de chercher le profiles.yml dans le dossier courant
    DBT_PROFILES_DIR=/dbt

WORKDIR /dbt

# Installation des dépendances système minimales (git est souvent requis pour dbt deps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Copie de l'environnement virtuel
COPY --from=builder /dbt/.venv /dbt/.venv

# Copie du projet dbt complet
COPY . .

# Commande par défaut : on lance un "dbt debug" pour vérifier la connexion
CMD ["dbt", "debug"]