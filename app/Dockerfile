# Etape 1 : Builder (On utilise l'image officielle de uv)
FROM ghcr.io/astral-sh/uv:python3.9-bookworm-slim AS builder

# Configuration pour que uv soit rapide et silencieux
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

# On copie uniquement les fichiers de dépendances pour profiter du cache Docker
# (Si tu utilises un requirements.txt généré ou un pyproject.toml)
COPY requirements.txt .

# Installation des dépendances dans un environnement virtuel (/app/.venv)
# --no-cache évite de garder les fichiers téléchargés dans l'image
RUN uv venv /app/.venv && \
    uv pip install --no-cache -r requirements.txt

# Etape 2 : Runtime (L'image finale, plus légère)
FROM python:3.9-slim-bookworm

# Variables d'environnement pour Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# On installe juste les outils système minimaux (si besoin de compiler quelque chose au runtime, sinon facultatif)
# Pour pyproj ou pandas, c'est souvent utile d'avoir les lib de base
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# On récupère l'environnement virtuel prêt à l'emploi depuis l'étape "builder"
COPY --from=builder /app/.venv /app/.venv

# On copie le code de l'application
COPY . .

# Vérification de santé (Healthcheck)
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Port par défaut
EXPOSE 8501

# Lancement de l'app
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]