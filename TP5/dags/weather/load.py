"""Chargement des mesures dans PostgreSQL (upsert idempotent)."""

from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather import config

log = logging.getLogger(__name__)


def load(rows: list[dict], run_id: str) -> dict:
    """Upsert des mesures sur la clé (city, measured_at) : pas de doublon en cas de relance."""
    sql = f"""
        INSERT INTO {config.WEATHER_TABLE}
            (city, measured_at, temp_c, humidity_pct, wind_kmh, fetched_at, run_id)
        VALUES
            (%(city)s, %(measured_at)s, %(temp_c)s, %(humidity_pct)s, %(wind_kmh)s, %(fetched_at)s, %(run_id)s)
        ON CONFLICT (city, measured_at) DO UPDATE SET
            temp_c = EXCLUDED.temp_c,
            humidity_pct = EXCLUDED.humidity_pct,
            wind_kmh = EXCLUDED.wind_kmh,
            fetched_at = EXCLUDED.fetched_at,
            run_id = EXCLUDED.run_id;
    """
    hook = PostgresHook(postgres_conn_id=config.PG_CONN_ID)
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.executemany(sql, [{**row, "run_id": run_id} for row in rows])
    conn.commit()
    log.info("load : %d mesures chargées dans %s", len(rows), config.WEATHER_TABLE)
    return {"rows_received": len(rows), "rows_inserted": len(rows)}
