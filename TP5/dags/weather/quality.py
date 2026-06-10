"""Contrôles qualité des mesures avant chargement."""

from __future__ import annotations

import logging

from weather import config

log = logging.getLogger(__name__)


def evaluate(rows: list[dict]) -> dict:
    """Vérifie chaque mesure et renvoie un rapport : valide + liste détaillée des anomalies."""
    issues: list[dict] = []
    for row in rows:
        issues.extend(_check_row(row))

    report = {"valid": not issues, "checked": len(rows), "issues": issues}
    if issues:
        log.warning("quality : %d anomalie(s) sur %d mesures", len(issues), len(rows))
    else:
        log.info("quality : %d mesures valides", len(rows))
    return report


def _check_row(row: dict) -> list[dict]:
    """Applique les bornes de plausibilité à une mesure."""
    found = []

    def fail(field, value, rule):
        found.append({"city": row["city"], "field": field, "value": value, "rule": rule})

    temp = row.get("temp_c")
    if temp is None or not (config.TEMP_MIN <= temp <= config.TEMP_MAX):
        fail("temp_c", temp, f"{config.TEMP_MIN}..{config.TEMP_MAX}")

    humidity = row.get("humidity_pct")
    if humidity is None or not (config.HUMIDITY_MIN <= humidity <= config.HUMIDITY_MAX):
        fail("humidity_pct", humidity, f"{config.HUMIDITY_MIN}..{config.HUMIDITY_MAX}")

    wind = row.get("wind_kmh")
    if wind is None or not (config.WIND_MIN <= wind <= config.WIND_MAX):
        fail("wind_kmh", wind, f"{config.WIND_MIN}..{config.WIND_MAX}")

    return found
