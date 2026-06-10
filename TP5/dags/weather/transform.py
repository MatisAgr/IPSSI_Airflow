"""Transformation des réponses brutes en lignes prêtes pour le chargement."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from weather import config

log = logging.getLogger(__name__)


def transform(raw_responses: list[dict]) -> list[dict]:
    """Sélectionne les champs utiles et structure une ligne par ville."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for raw in raw_responses:
        current = raw["current"]
        rows.append(
            {
                "city": raw["_city"],
                "measured_at": current["time"],
                "temp_c": current["temperature_2m"],
                "humidity_pct": current["relative_humidity_2m"],
                "wind_kmh": current["wind_speed_10m"],
                "fetched_at": fetched_at,
            }
        )

    if config.force_anomaly() and rows:
        rows[0]["temp_c"] = 999.0  # valeur hors plage pour la démonstration d'anomalie
        log.warning(
            "transform : anomalie qualité injectée (weather_force_anomaly=true) sur %s",
            rows[0]["city"],
        )

    log.info("transform : %d lignes structurées", len(rows))
    return rows
