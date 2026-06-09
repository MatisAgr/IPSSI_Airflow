# Champs retenus — Open-Meteo

## Réponse brute de l'API (extrait)

```json
{
  "latitude": 48.8566,
  "longitude": 2.3522,
  "generationtime_ms": 0.052,
  "utc_offset_seconds": 7200,
  "timezone": "Europe/Paris",
  "timezone_abbreviation": "CEST",
  "elevation": 38.0,
  "current_units": { "temperature_2m": "°C", "relative_humidity_2m": "%", "wind_speed_10m": "km/h" },
  "current": {
    "time": "2026-06-08T13:00",
    "temperature_2m": 21.4,
    "relative_humidity_2m": 58,
    "wind_speed_10m": 14.2
  }
}
```

## Champs retenus → table cible

| Champ API | Renommé en | Justification |
|---|---|---|
| `current.time` | `measured_at` | Horodatage de la mesure, clé temporelle de la table |
| `current.temperature_2m` | `temp_c` | Indicateur météo principal |
| `current.relative_humidity_2m` | `humidity_pct` | Indicateur de confort / précipitations |
| `current.wind_speed_10m` | `wind_kmh` | Indicateur de conditions météo |
| *(injecté)* `_city` | `city` | Identifiant ville, absent de la réponse API |
| *(calculé)* | `fetched_at` | Horodatage d'ingestion, utile pour la traçabilité |

## Champs écartés

| Champ | Raison |
|---|---|
| `generationtime_ms` | Métadonnée interne API, aucune valeur métier |
| `utc_offset_seconds` | Redondant avec `measured_at` qui contient déjà le fuseau |
| `elevation` | Non pertinent pour ce pipeline météo |
| `timezone` / `timezone_abbreviation` | Inutile dès lors qu'on conserve l'ISO 8601 |
| `*_units` | On normalise les noms de colonnes, pas besoin de stocker les unités |
