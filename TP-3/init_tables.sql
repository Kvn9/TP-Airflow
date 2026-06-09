-- TP 2B — Script d'initialisation des tables PostgreSQL

-- Table principale : données météo transformées
CREATE TABLE IF NOT EXISTS meteo_quotidienne (
    id                SERIAL PRIMARY KEY,
    ville             VARCHAR(100)   NOT NULL,
    date              DATE           NOT NULL,
    temperature_max   NUMERIC(5, 2),
    temperature_min   NUMERIC(5, 2),
    precipitation_mm  NUMERIC(6, 2),
    vent_max_kmh      NUMERIC(6, 2),
    ingere_le         TIMESTAMP      NOT NULL DEFAULT NOW(),
    UNIQUE (ville, date)
);

-- Table de suivi d'ingestion : traçabilité de chaque exécution DAG
CREATE TABLE IF NOT EXISTS suivi_ingestion (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(200)  NOT NULL,
    run_id          VARCHAR(200)  NOT NULL,
    villes          TEXT          NOT NULL,  -- liste JSON des villes traitées
    nb_lignes       INTEGER       NOT NULL,
    statut          VARCHAR(50)   NOT NULL,  -- 'succes' | 'erreur'
    message         TEXT,
    execute_le      TIMESTAMP     NOT NULL DEFAULT NOW()
);
