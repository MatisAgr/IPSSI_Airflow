"""Extraction depuis l'API Open-Meteo et archivage des réponses brutes."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

from weather import config

log = logging.getLogger(__name__)


def fetch_city(city: dict) -> dict:
    """Appelle l'API Open-Meteo pour une ville et renvoie la réponse JSON brute."""
    resp = requests.get(
        config.OPEN_METEO_URL,
        params={
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "auto",
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    data["_city"] = city["name"]  # l'API ne renvoie pas le nom, on l'injecte
    log.info("fetch_city[%s] -> HTTP %s", city["name"], resp.status_code)
    return data


def archive_raw(raw_responses: list[dict], run_id: str) -> list[str]:
    """Archive chaque réponse brute en JSON, un fichier par ville (écrasable -> idempotent)."""
    run_dir = Path(config.RAW_ARCHIVE_DIR) / _safe(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for raw in raw_responses:
        path = run_dir / f"{raw['_city'].lower()}.json"
        path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        paths.append(str(path))
        log.info("archive_raw[%s] -> %s", raw["_city"], path)
    return paths


def _safe(run_id: str) -> str:
    """Rend un run_id utilisable comme nom de dossier."""
    for ch in (":", "+", "/", "\\"):
        run_id = run_id.replace(ch, "_")
    return run_id
