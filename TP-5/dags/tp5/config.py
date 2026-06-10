"""
TP 5 — Configuration centrale du pipeline.

Toutes les constantes (villes, chemins, base de données, seuils qualité)
sont regroupées ici pour éviter le hardcode dispersé dans les tâches.
"""

import os

# ─────────────────────────────────────────────────────────────────────────
# Villes disponibles (coordonnées Open-Meteo)
# La liste réellement traitée par un run est définie via dag_run.conf
# (clé "villes"), avec ce dictionnaire comme valeur par défaut.
# ─────────────────────────────────────────────────────────────────────────
VILLES_DISPONIBLES = {
    "Paris":     {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon":      {"latitude": 45.7640, "longitude": 4.8357},
    "Bordeaux":  {"latitude": 44.8378, "longitude": -0.5792},
    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
    "Toulouse":  {"latitude": 43.6047, "longitude": 1.4442},
    "Nantes":    {"latitude": 47.2184, "longitude": -1.5536},
}

VILLES_PAR_DEFAUT = ["Paris", "Lyon", "Bordeaux"]
FORECAST_DAYS_PAR_DEFAUT = 7

CHAMPS_DAILY = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]

API_URL = "https://api.open-meteo.com/v1/forecast"
API_TIMEOUT_SECONDS = 10

# ─────────────────────────────────────────────────────────────────────────
# Archivage des données brutes (JSON) — répertoire monté en volume Docker
# ─────────────────────────────────────────────────────────────────────────
ARCHIVE_DIR = os.environ.get("TP5_ARCHIVE_DIR", "/opt/airflow/data/raw")

# ─────────────────────────────────────────────────────────────────────────
# Connexion PostgreSQL
# (en production : utiliser une Airflow Connection "postgres_meteo" via
#  PostgresHook plutôt qu'un dict en dur — voir README, section limites)
# ─────────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("TP5_DB_HOST", "postgres"),
    "port":     int(os.environ.get("TP5_DB_PORT", 5432)),
    "dbname":   os.environ.get("TP5_DB_NAME", "airflow"),
    "user":     os.environ.get("TP5_DB_USER", "airflow"),
    "password": os.environ.get("TP5_DB_PASSWORD", "airflow"),
}

# ─────────────────────────────────────────────────────────────────────────
# Seuils de contrôle qualité
# ─────────────────────────────────────────────────────────────────────────
QUALITE_TEMP_MIN = -50.0
QUALITE_TEMP_MAX = 60.0
QUALITE_PRECIP_MAX = 500.0
QUALITE_VENT_MAX = 250.0

CHAMPS_OBLIGATOIRES = [
    "ville",
    "date",
    "temperature_max",
    "temperature_min",
    "precipitation_mm",
    "vent_max_kmh",
]
