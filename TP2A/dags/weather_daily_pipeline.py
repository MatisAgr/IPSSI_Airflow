"""TP2A - Ingestion météo Open-Meteo : fetch (API réelle) → parse → validate → store."""
# TP2A — version avec appel API réel (Open-Meteo), pas de données simulées

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# Coordonnées fixes des villes (les noms viennent du .env via WEATHER_CITIES)
_COORDS = {
    "Paris":  {"lat": 48.8534, "lon":  2.3488},
    "Berlin": {"lat": 52.5244, "lon": 13.4105},
    "Madrid": {"lat": 40.4165, "lon": -3.7026},
}

# Chargement depuis le .env (injecté dans le conteneur par docker compose)
OPEN_METEO_URL = os.environ.get("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")
CITIES = [
    {"name": name, **_COORDS[name]}
    for name in os.environ.get("WEATHER_CITIES", "Paris,Berlin,Madrid").split(",")
    if name in _COORDS
]
OUTPUT_CSV = Path(os.environ.get("OUTPUT_CSV", "/opt/airflow/logs/weather_openmeteo.csv"))

@dag(
    dag_id="weather_daily_pipeline",
    description="TP2A Ingestion météo Open-Meteo (API réelle) - fetch / parse / validate / store",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tp2a", "weather", "open-meteo"],
    default_args={"owner": "matis", "retries": 3, "retry_delay": 300},
)
def weather_daily_pipeline():

    @task
    def init_log(**context) -> None:
        """Log les métadonnées du run au démarrage."""
        log.info("Pipeline démarré — run_id=%s  logical_date=%s", context["run_id"], context["logical_date"])

    @task
    def fetch_city(city: dict) -> dict:
        """Appelle l'API Open-Meteo pour une ville."""
        resp = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude":  city["lat"],
                "longitude": city["lon"],
                "current":   "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "timezone":  "auto",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        data["_city"] = city["name"]  # l'API ne retourne pas le nom, on l'injecte
        log.info("fetch_city[%s] -> HTTP 200", city["name"])
        return data

    @task
    def parse_weather(raw_responses: list[dict]) -> list[dict]:
        """Extrait uniquement les champs utiles et structure les données pour la table cible.

        Champs retenus (voir livrable/champs_retenus.md) :
          temperature_2m        -> temp_c        : température actuelle en °C
          relative_humidity_2m  -> humidity_pct  : humidité relative en %
          wind_speed_10m        -> wind_kmh      : vent à 10 m en km/h
          time                  -> measured_at   : horodatage de la mesure
        Champs écartés : generationtime_ms, elevation, utc_offset_seconds, *_units (méta API)
        """
        parsed = []
        fetched_at = datetime.now(timezone.utc).isoformat()
        for raw in raw_responses:
            current = raw["current"]
            parsed.append({
                "city":         raw["_city"],
                "measured_at":  current["time"],
                "temp_c":       current["temperature_2m"],
                "humidity_pct": current["relative_humidity_2m"],
                "wind_kmh":     current["wind_speed_10m"],
                "fetched_at":   fetched_at,
            })
        log.info("parse_weather : %d villes parsées -> %s", len(parsed), parsed)
        return parsed

    @task
    def validate_weather(parsed: list[dict]) -> list[dict]:
        """Vérifie la cohérence des données avant stockage."""
        for row in parsed:
            if not (-50 <= row["temp_c"] <= 60):
                raise ValueError(f"Température hors limites : {row}")
            if not (0 <= row["humidity_pct"] <= 100):
                raise ValueError(f"Humidité hors limites : {row}")
            if not (0 <= row["wind_kmh"] <= 500):
                raise ValueError(f"Vent hors limites : {row}")
        log.info("validate_weather : %d mesures valides", len(parsed))
        return parsed

    @task
    def store_weather(validated: list[dict]) -> None:
        """Sauvegarde en CSV (structure prête pour future insertion SQL)."""
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["city", "measured_at", "temp_c", "humidity_pct", "wind_kmh", "fetched_at"]
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(validated)
        log.info("store_weather : %d lignes -> %s", len(validated), OUTPUT_CSV)
        for row in validated:
            log.info("STORE  %-8s | %5.1f°C | %3s%% HR | %5.1f km/h | %s",
                     row["city"], row["temp_c"], row["humidity_pct"], row["wind_kmh"], row["measured_at"])

    @task(trigger_rule="one_failed")
    def send_alert() -> None:
        """Déclenché si au moins une tâche amont a échoué."""
        log.error("ALERTE pipeline Open-Meteo — une tâche a échoué, consulter les logs Airflow")

    # --- Dépendances ----------------------------------------------------------
    init = init_log()

    # expand crée une instance fetch_city[0/1/2] par ville, exécutées en parallèle
    raw = fetch_city.expand(city=CITIES)
    init >> raw  # toutes les instances attendent init_log

    parsed    = parse_weather(raw)       # reçoit la liste des 3 résultats via XCom
    validated = validate_weather(parsed)
    stored    = store_weather(validated)

    alert = send_alert()
    init >> alert
    raw >> alert
    parsed >> alert
    validated >> alert
    stored >> alert


weather_daily_pipeline()
