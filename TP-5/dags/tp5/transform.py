"""
TP 5 — Transformation des données brutes Open-Meteo.

Extrait uniquement les champs utiles au besoin métier et produit
des lignes "plates" prêtes pour le contrôle qualité / chargement.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def transformer_payload(ville: str, payload: dict) -> list[dict]:
    """
    Transforme la réponse brute Open-Meteo d'une ville en une liste de
    lignes structurées :

        {ville, date, temperature_max, temperature_min,
         precipitation_mm, vent_max_kmh}
    """
    daily = payload.get("daily", {})
    dates       = daily.get("time", [])
    temp_max    = daily.get("temperature_2m_max", [])
    temp_min    = daily.get("temperature_2m_min", [])
    precip      = daily.get("precipitation_sum", [])
    vent        = daily.get("windspeed_10m_max", [])

    lignes = []
    for i, date in enumerate(dates):
        lignes.append({
            "ville":            ville,
            "date":             date,
            "temperature_max":  temp_max[i] if i < len(temp_max) else None,
            "temperature_min":  temp_min[i] if i < len(temp_min) else None,
            "precipitation_mm": precip[i]   if i < len(precip)   else None,
            "vent_max_kmh":     vent[i]      if i < len(vent)      else None,
        })

    logger.info("[%s] %s lignes transformées", ville, len(lignes))
    return lignes
