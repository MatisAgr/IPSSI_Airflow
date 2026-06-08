"""TP2 - DAG meteo quotidien : init -> fetch -> validate -> transform -> store (CSV)."""

from __future__ import annotations

import csv
import logging
import random
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
log = logging.getLogger(__name__)

CITIES = ["Paris", "Berlin", "Madrid"]
OUTPUT_CSV = Path("/opt/airflow/logs/weather_output.csv")  # mappé sur ./logs/ sur l'hôte


@dag(
    dag_id="weather_daily_pipeline",
    description="Pipeline meteo quotidien - init / fetch / validate / transform / store",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tp2", "weather"],
    default_args={"owner": "matis_anger", "retries": 3, "retry_delay": 300},
)
def weather_daily_pipeline():

    @task
    def init_log(**context) -> None:
        """Log les métadonnées du run au démarrage du pipeline."""
        log.info("Pipeline démarré — run_id=%s  logical_date=%s", context["run_id"], context["logical_date"])

    @task
    def fetch_weather() -> list[dict]:
        """Génère des mesures météo simulées (pas d'appel API réel)."""
        raw = [
            {"city": c, "temp_c": round(random.uniform(5, 30), 1), "humidity": random.randint(30, 90)}
            for c in CITIES
        ]
        log.info("fetch_weather : %d villes -> %s", len(raw), raw)
        return raw

    @task
    def validate_weather(raw: list[dict]) -> list[dict]:
        """Vérifie que chaque mesure est dans des plages cohérentes."""
        for row in raw:
            if not (-50 <= row["temp_c"] <= 60):
                raise ValueError(f"Température hors limites : {row}")
            if not (0 <= row["humidity"] <= 100):
                raise ValueError(f"Humidité hors limites : {row}")
        log.info("validate_weather : %d mesures valides", len(raw))
        return raw

    @task
    def transform_weather(raw: list[dict]) -> list[dict]:
        """Convertit la température °C → °F."""
        clean = [
            {"city": r["city"], "temp_f": round(r["temp_c"] * 9 / 5 + 32, 1), "humidity": r["humidity"]}
            for r in raw
        ]
        log.info("transform_weather : %s", clean)
        return clean

    @task
    def store_weather(clean: list[dict]) -> None:
        """Sauvegarde les données transformées dans un fichier CSV."""
        # ancienne version log uniquement
        # for row in clean:
        #     log.info("STORE  city=%-8s temp_f=%-5s humidity=%s%%", row["city"], row["temp_f"], row["humidity"])

        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["city", "temp_f", "humidity"])
            writer.writeheader()
            writer.writerows(clean)
        log.info("store_weather : %d lignes écrites -> %s", len(clean), OUTPUT_CSV)

    @task(trigger_rule="one_failed")
    def send_alert() -> None:
        """Déclenché automatiquement si au moins une tâche amont a échoué."""
        log.error("ALERTE pipeline météo — une tâche a échoué, consulter les logs Airflow")


# cablage du graph de dépendances
    init = init_log()
    raw = fetch_weather()
    init >> raw  # init ne retourne pas de valeur, dépendance explicite

    validated = validate_weather(raw)
    clean = transform_weather(validated)
    stored = store_weather(clean)

    # send_alert dépend de toutes les étapes : se déclenche si l'une d'elles échoue
    alert = send_alert()
    init >> alert
    raw >> alert
    validated >> alert
    clean >> alert
    stored >> alert


weather_daily_pipeline()
