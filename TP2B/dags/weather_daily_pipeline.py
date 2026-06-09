"""TP2B - Pipeline complet Open-Meteo : extract -> transform -> load PostgreSQL + traçabilité."""
# Récupération API -> transformation -> chargement PostgreSQL -> ligne de suivi d'ingestion

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# --- Paramètres techniques (lus depuis le .env, pas de hardcode) ---
PG_CONN_ID = "weather_db"  # mappé sur AIRFLOW_CONN_WEATHER_DB
OPEN_METEO_URL = os.environ.get("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")
WEATHER_TABLE = os.environ.get("WEATHER_TABLE", "weather_measurements")
INGESTION_TABLE = os.environ.get("INGESTION_TABLE", "ingestion_log")
SCHEMA_SQL = Path("/opt/airflow/sql/schema.sql")

# --- Paramètres métier ---
_COORDS = {
    "Paris":  {"lat": 48.8534, "lon":  2.3488},
    "Berlin": {"lat": 52.5244, "lon": 13.4105},
    "Madrid": {"lat": 40.4165, "lon": -3.7026},
}
CITIES = [
    {"name": name, **_COORDS[name]}
    for name in os.environ.get("WEATHER_CITIES", "Paris,Berlin,Madrid").split(",")
    if name in _COORDS
]


@dag(
    dag_id="weather_daily_pipeline",
    description="TP2B — Pipeline Open-Meteo -> PostgreSQL (extract / transform / load / suivi)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["tp2b", "weather", "postgres", "etl"],
    default_args={"owner": "matis", "retries": 3, "retry_delay": 300},
)
def weather_daily_pipeline():

    @task
    def init_log(**context) -> None:
        """Log les métadonnées du run au démarrage."""
        log.info("Pipeline démarré — run_id=%s  data_interval=%s -> %s",
                 context["run_id"], context["data_interval_start"], context["data_interval_end"])

    @task
    def create_tables() -> None:
        """Crée les tables cible et de suivi si elles n'existent pas (idempotent)."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(SCHEMA_SQL.read_text())
        log.info("create_tables : schéma appliqué (%s, %s)", WEATHER_TABLE, INGESTION_TABLE)

    @task
    def fetch_city(city: dict) -> dict:
        """EXTRACT — appelle l'API Open-Meteo pour une ville (une instance par ville)."""
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
        data["_city"] = city["name"]  # l'API ne renvoie pas le nom, on l'injecte
        log.info("fetch_city[%s] -> HTTP 200", city["name"])
        return data

    @task
    def transform_weather(raw_responses: list[dict]) -> list[dict]:
        """TRANSFORM — sélectionne les champs utiles et structure pour la table cible."""
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = [
            {
                "city":         raw["_city"],
                "measured_at":  raw["current"]["time"],
                "temp_c":       raw["current"]["temperature_2m"],
                "humidity_pct": raw["current"]["relative_humidity_2m"],
                "wind_kmh":     raw["current"]["wind_speed_10m"],
                "fetched_at":   fetched_at,
            }
            for raw in raw_responses
        ]
        log.info("transform_weather : %d lignes structurées", len(rows))
        return rows

    @task
    def validate_weather(rows: list[dict]) -> list[dict]:
        """Contrôle la cohérence des mesures avant chargement."""
        for r in rows:
            if not (-50 <= r["temp_c"] <= 60):
                raise ValueError(f"Température hors limites : {r}")
            if not (0 <= r["humidity_pct"] <= 100):
                raise ValueError(f"Humidité hors limites : {r}")
            if not (0 <= r["wind_kmh"] <= 500):
                raise ValueError(f"Vent hors limites : {r}")
        log.info("validate_weather : %d mesures valides", len(rows))
        return rows

    @task
    def load_weather(rows: list[dict], **context) -> dict:
        """LOAD — charge les mesures dans PostgreSQL (upsert sur ville + horodatage)."""
        run_id = context["run_id"]
        sql = f"""
            INSERT INTO {WEATHER_TABLE} (city, measured_at, temp_c, humidity_pct, wind_kmh, fetched_at, run_id)
            VALUES (%(city)s, %(measured_at)s, %(temp_c)s, %(humidity_pct)s, %(wind_kmh)s, %(fetched_at)s, %(run_id)s)
            ON CONFLICT (city, measured_at) DO UPDATE SET
                temp_c = EXCLUDED.temp_c,
                humidity_pct = EXCLUDED.humidity_pct,
                wind_kmh = EXCLUDED.wind_kmh,
                fetched_at = EXCLUDED.fetched_at,
                run_id = EXCLUDED.run_id;
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, [{**r, "run_id": run_id} for r in rows])
        conn.commit()
        log.info("load_weather : %d lignes chargées dans %s", len(rows), WEATHER_TABLE)
        return {"rows_received": len(rows), "rows_inserted": len(rows)}

    @task
    def record_ingestion(load_result: dict, **context) -> None:
        """Traçabilité — écrit une ligne de succès dans la table de suivi d'ingestion."""
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(
            f"""INSERT INTO {INGESTION_TABLE}
                (run_id, source, interval_start, interval_end, status, rows_received, rows_inserted, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            parameters=(
                context["run_id"], "open-meteo",
                context["data_interval_start"], context["data_interval_end"],
                "success", load_result["rows_received"], load_result["rows_inserted"], None,
            ),
        )
        log.info("record_ingestion : run %s marqué 'success'", context["run_id"])

    @task(trigger_rule="one_failed")
    def record_failure(**context) -> None:
        """Traçabilité — écrit une ligne d'échec si une tâche amont a échoué."""
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(SCHEMA_SQL.read_text())  # garantit l'existence de la table même si create_tables a échoué
        hook.run(
            f"""INSERT INTO {INGESTION_TABLE}
                (run_id, source, interval_start, interval_end, status, rows_received, rows_inserted, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            parameters=(
                context["run_id"], "open-meteo",
                context["data_interval_start"], context["data_interval_end"],
                "failed", None, None, "Une tâche amont a échoué — voir logs Airflow",
            ),
        )
        log.error("record_failure : run %s marqué 'failed'", context["run_id"])

    # Orchestration (séparation extract / transform / load / suivi) 
    init = init_log()
    tables = create_tables()

    raw = fetch_city.expand(city=CITIES)          # EXTRACT en parallèle (1 instance/ville)
    transformed = transform_weather(raw)          # TRANSFORM
    validated = validate_weather(transformed)
    loaded = load_weather(validated)              # LOAD PostgreSQL
    recorded = record_ingestion(loaded)           # suivi succès

    init >> [tables, raw]
    tables >> loaded                              # le chargement attend l'existence des tables

    # suivi d'échec : déclenché si n'importe quelle tâche amont échoue
    failure = record_failure()
    [init, tables, raw, transformed, validated, loaded, recorded] >> failure


weather_daily_pipeline()
