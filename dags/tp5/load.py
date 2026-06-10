"""
TP 5 — Chargement PostgreSQL.

Toutes les écritures utilisent `ON CONFLICT ... DO UPDATE` afin de
garantir l'idempotence : une relance avec les mêmes données (même
ville, même date, même run_id) écrase la ligne existante au lieu d'en
créer une nouvelle.
"""

from __future__ import annotations

import logging

import psycopg2

from tp5.config import DB_CONFIG

logger = logging.getLogger(__name__)


def _connexion():
    return psycopg2.connect(**DB_CONFIG)


def charger_lignes_valides(lignes: list[dict], run_id: str) -> int:
    """Insère/maj les lignes validées dans meteo_quotidienne. Idempotent."""
    if not lignes:
        logger.info("Aucune ligne valide à charger.")
        return 0

    sql = """
        INSERT INTO meteo_quotidienne
            (ville, date, temperature_max, temperature_min,
             precipitation_mm, vent_max_kmh, run_id)
        VALUES
            (%(ville)s, %(date)s, %(temperature_max)s, %(temperature_min)s,
             %(precipitation_mm)s, %(vent_max_kmh)s, %(run_id)s)
        ON CONFLICT (ville, date)
        DO UPDATE SET
            temperature_max  = EXCLUDED.temperature_max,
            temperature_min  = EXCLUDED.temperature_min,
            precipitation_mm = EXCLUDED.precipitation_mm,
            vent_max_kmh     = EXCLUDED.vent_max_kmh,
            run_id           = EXCLUDED.run_id,
            ingere_le        = NOW();
    """

    parametres = [{**ligne, "run_id": run_id} for ligne in lignes]

    conn = _connexion()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, parametres)
        logger.info("%s lignes chargées/mises à jour dans meteo_quotidienne", len(lignes))
    finally:
        conn.close()

    return len(lignes)


def tracer_anomalies(anomalies: list[dict], run_id: str) -> int:
    """Insère les anomalies détectées dans meteo_anomalies. Idempotent."""
    if not anomalies:
        return 0

    sql = """
        INSERT INTO meteo_anomalies
            (ville, date, champ, valeur, regle_violee, run_id)
        VALUES
            (%(ville)s, %(date)s, %(champ)s, %(valeur)s, %(regle_violee)s, %(run_id)s)
        ON CONFLICT (ville, date, champ, run_id)
        DO UPDATE SET
            valeur       = EXCLUDED.valeur,
            regle_violee = EXCLUDED.regle_violee,
            detecte_le   = NOW();
    """

    parametres = [{**a, "run_id": run_id} for a in anomalies]

    conn = _connexion()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, parametres)
        logger.info("%s anomalies tracées dans meteo_anomalies", len(anomalies))
    finally:
        conn.close()

    return len(anomalies)
