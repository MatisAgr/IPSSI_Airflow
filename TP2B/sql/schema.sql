-- TP2B — Schéma de la base métier (weather-db)
-- Exécuté (idempotent) par la tâche create_tables du DAG.

-- Table cible : une ligne par mesure météo (ville + horodatage)
CREATE TABLE IF NOT EXISTS weather_measurements (
    id           SERIAL PRIMARY KEY,
    city         TEXT        NOT NULL,
    measured_at  TIMESTAMP   NOT NULL,      -- heure locale de la mesure (renvoyée par l'API)
    temp_c       REAL        NOT NULL,
    humidity_pct INTEGER     NOT NULL,
    wind_kmh     REAL        NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL,      -- horodatage d'ingestion (UTC)
    run_id       TEXT        NOT NULL,      -- run Airflow ayant produit la ligne
    UNIQUE (city, measured_at)              -- permet l'upsert : pas de doublon sur re-run
);

-- Table de suivi d'ingestion : une ligne par run (traçabilité)
CREATE TABLE IF NOT EXISTS ingestion_log (
    id             SERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL,
    source         TEXT        NOT NULL,    -- ex: open-meteo
    interval_start TIMESTAMPTZ,             -- début de la période de données (data interval)
    interval_end   TIMESTAMPTZ,             -- fin de la période de données
    status         TEXT        NOT NULL,    -- success | failed
    rows_received  INTEGER,                 -- lignes reçues de la source
    rows_inserted  INTEGER,                 -- lignes effectivement chargées
    error          TEXT,                    -- message d'erreur éventuel
    logged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
