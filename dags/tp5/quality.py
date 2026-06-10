"""
TP 5 — Contrôle qualité.

Règles appliquées à chaque ligne transformée. Toute violation est
remontée sous forme d'anomalie (ville, date, champ, règle violée).

Décision de branchement : si AU MOINS UNE anomalie est détectée sur le
batch, le pipeline n'effectue AUCUN chargement dans `meteo_quotidienne`
(cf. consigne : "ne doit pas charger les données finales") et trace
toutes les anomalies dans `meteo_anomalies`.
"""

from __future__ import annotations

import logging

from tp5.config import (
    CHAMPS_OBLIGATOIRES,
    QUALITE_TEMP_MIN,
    QUALITE_TEMP_MAX,
    QUALITE_PRECIP_MAX,
    QUALITE_VENT_MAX,
)

logger = logging.getLogger(__name__)


def controler_ligne(ligne: dict) -> list[dict]:
    """
    Contrôle une ligne et retourne la liste des anomalies trouvées.
    Chaque anomalie : {champ, valeur, regle_violee}.
    """
    anomalies = []

    # 1. Champs obligatoires non nuls
    for champ in CHAMPS_OBLIGATOIRES:
        if ligne.get(champ) is None:
            anomalies.append({
                "champ": champ,
                "valeur": None,
                "regle_violee": f"{champ} est NULL (champ obligatoire)",
            })

    # Si valeurs manquantes, inutile de pousser plus loin les contrôles numériques
    if anomalies:
        return anomalies

    temp_max = ligne["temperature_max"]
    temp_min = ligne["temperature_min"]
    precip   = ligne["precipitation_mm"]
    vent     = ligne["vent_max_kmh"]

    # 2. Plages physiquement plausibles
    if not (QUALITE_TEMP_MIN <= temp_max <= QUALITE_TEMP_MAX):
        anomalies.append({
            "champ": "temperature_max",
            "valeur": temp_max,
            "regle_violee": f"hors plage [{QUALITE_TEMP_MIN}, {QUALITE_TEMP_MAX}]",
        })

    if not (QUALITE_TEMP_MIN <= temp_min <= QUALITE_TEMP_MAX):
        anomalies.append({
            "champ": "temperature_min",
            "valeur": temp_min,
            "regle_violee": f"hors plage [{QUALITE_TEMP_MIN}, {QUALITE_TEMP_MAX}]",
        })

    # 3. Cohérence min <= max
    if temp_min is not None and temp_max is not None and temp_min > temp_max:
        anomalies.append({
            "champ": "temperature_min/max",
            "valeur": f"min={temp_min}, max={temp_max}",
            "regle_violee": "temperature_min > temperature_max",
        })

    # 4. Précipitations et vent positifs et plausibles
    if not (0 <= precip <= QUALITE_PRECIP_MAX):
        anomalies.append({
            "champ": "precipitation_mm",
            "valeur": precip,
            "regle_violee": f"hors plage [0, {QUALITE_PRECIP_MAX}]",
        })

    if not (0 <= vent <= QUALITE_VENT_MAX):
        anomalies.append({
            "champ": "vent_max_kmh",
            "valeur": vent,
            "regle_violee": f"hors plage [0, {QUALITE_VENT_MAX}]",
        })

    return anomalies


def controler_qualite(lignes: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Contrôle toutes les lignes.

    Retourne (lignes_valides, anomalies) où `anomalies` est une liste de
    dicts {ville, date, champ, valeur, regle_violee} prête pour
    insertion dans `meteo_anomalies`.
    """
    lignes_valides = []
    anomalies = []

    for ligne in lignes:
        erreurs = controler_ligne(ligne)
        if erreurs:
            for err in erreurs:
                anomalies.append({
                    "ville": ligne.get("ville"),
                    "date": ligne.get("date"),
                    "champ": err["champ"],
                    "valeur": str(err["valeur"]),
                    "regle_violee": err["regle_violee"],
                })
            logger.warning("[%s %s] %s anomalie(s) détectée(s) : %s",
                            ligne.get("ville"), ligne.get("date"), len(erreurs), erreurs)
        else:
            lignes_valides.append(ligne)

    logger.info("Contrôle qualité : %s lignes valides / %s anomalies",
                 len(lignes_valides), len(anomalies))
    return lignes_valides, anomalies


def injecter_anomalie(lignes: list[dict]) -> list[dict]:
    """
    Corrompt volontairement la première ligne du batch pour simuler une
    anomalie qualité (utilisé via dag_run.conf {"simuler_anomalie": true}).
    """
    if not lignes:
        return lignes

    lignes[0] = dict(lignes[0])  # copie pour ne pas muter l'original partagé
    lignes[0]["temperature_max"] = 999.9  # hors plage physique
    logger.warning(
        "Anomalie simulée injectée sur %s / %s : temperature_max=999.9",
        lignes[0]["ville"], lignes[0]["date"],
    )
    return lignes
