"""TP2 - DAG meteo quotidien : fetch -> transform -> store."""

from __future__ import annotations

import logging
import random
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

CITIES = ["Paris", "Berlin", "Madrid"]


@dag(
    dag_id="weather_daily_pipeline",
    description="Pipeline meteo quotidien - fetch / transform / store",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tp2", "weather"],
    default_args={"owner": "data_eng", "retries": 1},
)
def weather_daily_pipeline():

    @task
    def fetch_weather() -> list[dict]:
        """Recupere les mesures meteo en mock."""
        raw = [
            {"city": c, "temp_c": round(random.uniform(5, 30), 1), "humidity": random.randint(30, 90)}
            for c in CITIES
        ]
        log.info("fetch_weather : %d villes recuperees -> %s", len(raw), raw)
        return raw

    @task
    def transform_weather(raw: list[dict]) -> list[dict]:
        """Valide les champs cles mock et convertit la temperature en Fahrenheit."""
        clean: list[dict] = []
        for row in raw:
            missing = {"city", "temp_c", "humidity"} - row.keys()
            if missing:
                raise ValueError(f"Champs manquants {missing} dans {row}")
            clean.append(
                {"city": row["city"], "temp_f": round(row["temp_c"] * 9 / 5 + 32, 1), "humidity": row["humidity"]}
            )
        log.info("transform_weather : %d mesures validees -> %s", len(clean), clean)
        return clean

    @task
    def store_weather(clean: list[dict]) -> None:
        """Charge le resultat consolide en mock"""
        log.info("store_weather : chargement de %d enregistrements.", len(clean))
        for row in clean:
            log.info("STORE  city=%-8s temp_f=%-5s humidity=%s%%", row["city"], row["temp_f"], row["humidity"])

    # Dependances explicites : fetch -> transform -> store
    store_weather(transform_weather(fetch_weather()))


weather_daily_pipeline()
