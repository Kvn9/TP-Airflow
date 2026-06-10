-- TP 5 — Initialisation des tables PostgreSQL
-- Idempotent : peut être rejoué sans erreur (CREATE TABLE IF NOT EXISTS)

-- ─────────────────────────────────────────────────────────────────────────
-- Table 1 : données météo validées (chargées uniquement si qualité OK)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meteo_quotidienne (
    id                SERIAL PRIMARY KEY,
    ville             VARCHAR(100)  NOT NULL,
    date              DATE          NOT NULL,
    temperature_max   NUMERIC(5, 2),
    temperature_min   NUMERIC(5, 2),
    precipitation_mm  NUMERIC(6, 2),
    vent_max_kmh      NUMERIC(6, 2),
    run_id            VARCHAR(250)  NOT NULL,
    ingere_le         TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE (ville, date)
);

-- ─────────────────────────────────────────────────────────────────────────
-- Table 2 : anomalies qualité détectées (données NON chargées dans la table 1)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meteo_anomalies (
    id              SERIAL PRIMARY KEY,
    ville           VARCHAR(100)  NOT NULL,
    date            DATE          NOT NULL,
    champ           VARCHAR(100)  NOT NULL,
    valeur          TEXT,
    regle_violee    TEXT          NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    detecte_le      TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE (ville, date, champ, run_id)
);

-- ─────────────────────────────────────────────────────────────────────────
-- Table 3 : traçabilité d'ingestion — une ligne par exécution du DAG
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS suivi_ingestion (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(200)  NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    villes          TEXT          NOT NULL,
    nb_lignes_total     INTEGER   NOT NULL DEFAULT 0,
    nb_lignes_valides   INTEGER   NOT NULL DEFAULT 0,
    nb_anomalies        INTEGER   NOT NULL DEFAULT 0,
    statut          VARCHAR(50)   NOT NULL,   -- 'succes' | 'anomalie_qualite' | 'erreur'
    message         TEXT,
    execute_le      TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE (dag_id, run_id)
);
