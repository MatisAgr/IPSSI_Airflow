-- TP5 - Schéma de la base métier (weather)
-- Appliqué de manière idempotente par la tâche ensure_schema du DAG.

-- Table cible : une ligne par mesure (ville + horodatage)
CREATE TABLE IF NOT EXISTS weather_measurements (
    id           SERIAL PRIMARY KEY,
    city         TEXT        NOT NULL,
    measured_at  TIMESTAMP   NOT NULL,      -- heure locale de la mesure (API)
    temp_c       REAL        NOT NULL,
    humidity_pct INTEGER     NOT NULL,
    wind_kmh     REAL        NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL,      -- horodatage d'ingestion (UTC)
    run_id       TEXT        NOT NULL,      -- run Airflow ayant produit la ligne
    UNIQUE (city, measured_at)              -- clé d'upsert : pas de doublon en relance
);

-- Table de suivi : une ligne par run (traçabilité d'ingestion)
CREATE TABLE IF NOT EXISTS ingestion_log (
    id             SERIAL PRIMARY KEY,
    run_id         TEXT        NOT NULL UNIQUE,  -- clé d'upsert : un run = une ligne
    source         TEXT        NOT NULL,         -- ex : open-meteo
    interval_start TIMESTAMPTZ,
    interval_end   TIMESTAMPTZ,
    status         TEXT        NOT NULL,         -- success | anomaly | failed
    rows_received  INTEGER,
    rows_inserted  INTEGER,
    error          TEXT,
    logged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Table d'anomalies : détail des contrôles qualité en échec (par run / ville / champ)
CREATE TABLE IF NOT EXISTS quality_issues (
    id        SERIAL PRIMARY KEY,
    run_id    TEXT        NOT NULL,
    city      TEXT        NOT NULL,
    field     TEXT        NOT NULL,             -- champ en cause (temp_c, humidity_pct, wind_kmh)
    value     TEXT,                             -- valeur fautive
    rule      TEXT        NOT NULL,             -- borne attendue (ex : -50.0..60.0)
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
