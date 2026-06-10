"""TP5 -  extract -> archive brut -> transform -> contrôle qualité -> branchement (load | trace anomalie) -> traçabilité d'ingestion."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from weather import config, extract, load, quality, tracing, transform

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "matis",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="weather_open_meteo_pipeline",
    description="TP5 - Pipeline Open-Meteo industrialisé (extract / archive / transform / qualité / load / trace)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tp5", "weather", "open-meteo", "postgres", "etl"],
)
def weather_open_meteo_pipeline():

    @task
    def log_run_start(**context) -> None:
        """Journalise les métadonnées du run au démarrage."""
        log.info(
            "run_id=%s interval=%s -> %s",
            context["run_id"],
            context["data_interval_start"],
            context["data_interval_end"],
        )

    @task
    def ensure_schema() -> None:
        """Crée les tables cible et de suivi si absentes (idempotent)."""
        tracing.ensure_schema()

    @task
    def list_cities() -> list[dict]:
        """Lit la Variable Airflow `weather_cities` et renvoie les villes à traiter."""
        cities = config.get_cities()
        log.info("villes à traiter : %s", [c["name"] for c in cities])
        return cities

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_city(city: dict) -> dict:
        """EXTRACT - appel API Open-Meteo (une instance par ville)."""
        return extract.fetch_city(city)

    @task
    def archive_raw(raw_responses: list[dict], **context) -> list[str]:
        """ARCHIVE - écrit les réponses brutes sur disque (un JSON par ville)."""
        return extract.archive_raw(raw_responses, context["run_id"])

    @task
    def transform_measurements(raw_responses: list[dict]) -> list[dict]:
        """TRANSFORM - structure une ligne par ville pour la table cible."""
        return transform.transform(raw_responses)

    @task
    def check_quality(rows: list[dict]) -> dict:
        """Contrôle qualité - renvoie un rapport (valide + anomalies)."""
        return quality.evaluate(rows)

    @task.branch
    def branch_on_quality(report: dict) -> str:
        """Branchement conditionnel : charge si valide, sinon trace l'anomalie."""
        target = "load_measurements" if report["valid"] else "trace_anomaly"
        log.info("branch_on_quality : valid=%s -> %s", report["valid"], target)
        return target

    @task
    def load_measurements(rows: list[dict], **context) -> dict:
        """LOAD - upsert des mesures dans PostgreSQL."""
        return load.load(rows, context["run_id"])

    @task
    def record_success(result: dict, **context) -> None:
        """Traçabilité - ligne 'success' dans le journal d'ingestion."""
        tracing.record_run(
            context["run_id"], "open-meteo",
            context["data_interval_start"], context["data_interval_end"],
            "success", result["rows_received"], result["rows_inserted"], None,
        )

    @task
    def trace_anomaly(report: dict, **context) -> None:
        """Traçabilité - trace l'anomalie, marque 'anomaly', ne charge rien."""
        tracing.record_quality_issues(context["run_id"], report["issues"])
        tracing.record_run(
            context["run_id"], "open-meteo",
            context["data_interval_start"], context["data_interval_end"],
            "anomaly", report["checked"], 0,
            f"{len(report['issues'])} anomalie(s) qualité - chargement bloqué",
        )

    @task(trigger_rule="one_failed")
    def trace_failure(**context) -> None:
        """Traçabilité - ligne 'failed' si une tâche amont échoue (gestion d'erreur)."""
        tracing.ensure_schema()  # garantit la table même si ensure_schema a échoué
        tracing.record_run(
            context["run_id"], "open-meteo",
            context["data_interval_start"], context["data_interval_end"],
            "failed", None, None, "Une tâche amont a échoué - voir logs Airflow",
        )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Orchestration
    init = log_run_start()
    schema = ensure_schema()
    cities = list_cities()

    raw = extract_city.expand(city=cities)  # EXTRACT en  parallèle (1 instance/ville)
    archived = archive_raw(raw)             # ARCHIVE brut
    rows = transform_measurements(raw)      # TRANSFORM pour table cible
    report = check_quality(rows)            # CHECK qualité
    branch = branch_on_quality(report)      # BRANCHEMENT

    loaded = load_measurements(rows)        # LOAD si qualité ok
    success = record_success(loaded)
    anomaly = trace_anomaly(report)         # branche anomalie
    failure = trace_failure()               # gestion d'erreur

    init >> [schema, cities]
    cities >> raw >> [archived, rows]
    rows >> report >> branch
    schema >> [loaded, anomaly]             # écritures BDD attendent le schéma
    branch >> [loaded, anomaly]
    loaded >> success
    [success, anomaly] >> end
    [init, schema, cities, raw, archived, rows, report, loaded, success, anomaly] >> failure


weather_open_meteo_pipeline()
