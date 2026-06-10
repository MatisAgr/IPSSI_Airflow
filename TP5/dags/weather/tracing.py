"""Traçabilité d'ingestion : schéma, journal des runs et détail des anomalies qualité."""

from __future__ import annotations

import logging
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather import config

log = logging.getLogger(__name__)


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=config.PG_CONN_ID)


def ensure_schema() -> None:
    """Applique le schéma SQL (idempotent, CREATE TABLE IF NOT EXISTS)."""
    _hook().run(Path(config.SCHEMA_SQL).read_text(encoding="utf-8"))
    log.info(
        "schéma appliqué (%s, %s, %s)",
        config.WEATHER_TABLE,
        config.INGESTION_TABLE,
        config.QUALITY_TABLE,
    )


def record_run(
    run_id,
    source,
    interval_start,
    interval_end,
    status,
    rows_received,
    rows_inserted,
    error,
) -> None:
    """Upsert d'une ligne de suivi par run (idempotent sur run_id)."""
    _hook().run(
        f"""
        INSERT INTO {config.INGESTION_TABLE}
            (run_id, source, interval_start, interval_end, status, rows_received, rows_inserted, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            rows_received = EXCLUDED.rows_received,
            rows_inserted = EXCLUDED.rows_inserted,
            error = EXCLUDED.error,
            logged_at = now();
        """,
        parameters=(run_id, source, interval_start, interval_end, status, rows_received, rows_inserted, error),
    )
    log.info("ingestion_log : run %s -> %s", run_id, status)


def record_quality_issues(run_id, issues: list[dict]) -> None:
    """Trace le détail des anomalies qualité (purge d'abord le run -> idempotent)."""
    hook = _hook()
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {config.QUALITY_TABLE} WHERE run_id = %s", (run_id,))
        if issues:
            cur.executemany(
                f"""INSERT INTO {config.QUALITY_TABLE} (run_id, city, field, value, rule)
                    VALUES (%(run_id)s, %(city)s, %(field)s, %(value)s, %(rule)s)""",
                [
                    {
                        "run_id": run_id,
                        "city": i["city"],
                        "field": i["field"],
                        "value": str(i["value"]),
                        "rule": i["rule"],
                    }
                    for i in issues
                ],
            )
    conn.commit()
    log.info("quality_issues : %d ligne(s) tracée(s) pour run %s", len(issues), run_id)
