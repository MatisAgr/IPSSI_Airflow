"""Configuration centrale : Variables Airflow, connexion, constantes et seuils qualité."""

from __future__ import annotations

import json
import logging
import os

from airflow.models import Variable

log = logging.getLogger(__name__)

# Connexion Airflow vers la base métier (utilisée par PostgresHook)
PG_CONN_ID = "weather_db"

# Paramètres techniques (infrastructure), lus depuis l'environnement
OPEN_METEO_URL = os.environ.get("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")
WEATHER_TABLE = os.environ.get("WEATHER_TABLE", "weather_measurements")
INGESTION_TABLE = os.environ.get("INGESTION_TABLE", "ingestion_log")
QUALITY_TABLE = os.environ.get("QUALITY_TABLE", "quality_issues")
SCHEMA_SQL = "/opt/airflow/sql/schema.sql"
RAW_ARCHIVE_DIR = "/opt/airflow/data/raw"

# Seuils de contrôle qualité
TEMP_MIN, TEMP_MAX = -50.0, 60.0
HUMIDITY_MIN, HUMIDITY_MAX = 0, 100
WIND_MIN, WIND_MAX = 0.0, 500.0

# Référentiel des villes connues (coordonnées)
CITY_COORDS = {
    "Paris": {"lat": 48.8534, "lon": 2.3488},
    "Berlin": {"lat": 52.5244, "lon": 13.4105},
    "Madrid": {"lat": 40.4165, "lon": -3.7026},
}


def get_cities() -> list[dict]:
    """Villes à traiter, définies par la Variable Airflow `weather_cities` (liste JSON ou CSV)."""
    raw = Variable.get("weather_cities", default_var=None)
    if raw:
        try:
            names = json.loads(raw)
        except json.JSONDecodeError:
            names = [n.strip() for n in raw.split(",")]
    else:
        names = list(CITY_COORDS)
    cities = [{"name": n, **CITY_COORDS[n]} for n in names if n in CITY_COORDS]
    unknown = [n for n in names if n not in CITY_COORDS]
    if unknown:
        log.warning("Villes inconnues ignorées (absentes du référentiel) : %s", unknown)
    return cities


def force_anomaly() -> bool:
    """Toggle de démonstration : injecte une anomalie qualité si la Variable vaut 'true'."""
    return Variable.get("weather_force_anomaly", default_var="false").strip().lower() == "true"
