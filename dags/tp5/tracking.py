"""
TP 5 — Traçabilité d'ingestion.

Une ligne par exécution du DAG dans `suivi_ingestion`, écrite quel que
soit le résultat (succès ou anomalie qualité), grâce à `ON CONFLICT
(dag_id, run_id) DO UPDATE` : une relance du même run_id met à jour la
ligne existante au lieu d'en créer une nouvelle (idempotence).
"""

from __future__ import annotations

import json
import logging

import psycopg2

from tp5.config import DB_CONFIG

logger = logging.getLogger(__name__)


def ecrire_suivi(dag_id: str, run_id: str, villes: list[str],
                  nb_total: int, nb_valides: int, nb_anomalies: int,
                  statut: str, message: str) -> None:
    sql = """
        INSERT INTO suivi_ingestion
            (dag_id, run_id, villes, nb_lignes_total, nb_lignes_valides,
             nb_anomalies, statut, message)
        VALUES
            (%(dag_id)s, %(run_id)s, %(villes)s, %(nb_total)s, %(nb_valides)s,
             %(nb_anomalies)s, %(statut)s, %(message)s)
        ON CONFLICT (dag_id, run_id)
        DO UPDATE SET
            villes            = EXCLUDED.villes,
            nb_lignes_total   = EXCLUDED.nb_lignes_total,
            nb_lignes_valides = EXCLUDED.nb_lignes_valides,
            nb_anomalies      = EXCLUDED.nb_anomalies,
            statut            = EXCLUDED.statut,
            message           = EXCLUDED.message,
            execute_le        = NOW();
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "villes": json.dumps(villes),
                    "nb_total": nb_total,
                    "nb_valides": nb_valides,
                    "nb_anomalies": nb_anomalies,
                    "statut": statut,
                    "message": message,
                })
        logger.info("Suivi ingestion écrit — run_id=%s statut=%s", run_id, statut)
    finally:
        conn.close()
