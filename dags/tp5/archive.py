"""
TP 5 — Archivage des données brutes.

Chaque réponse API est sauvegardée telle quelle (JSON brut, non modifié)
sur disque, avant toute transformation.

Idempotence : le nom de fichier inclut la date logique d'exécution
(``ds``) et la ville. Une relance du même run écrase le même fichier
au lieu d'en créer un nouveau → pas d'accumulation de doublons.
"""

import json
import logging
import os

from tp5.config import ARCHIVE_DIR

logger = logging.getLogger(__name__)


def chemin_archive(ville: str, ds: str) -> str:
    nom_fichier = f"{ds}_{ville.lower()}.json"
    return os.path.join(ARCHIVE_DIR, ds, nom_fichier)


def archiver_payload(ville: str, ds: str, payload: dict) -> str:
    """Écrit le JSON brut sur disque. Retourne le chemin écrit."""
    chemin = chemin_archive(ville, ds)
    os.makedirs(os.path.dirname(chemin), exist_ok=True)

    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("[%s] payload brut archivé -> %s", ville, chemin)
    return chemin


def lire_archive(ville: str, ds: str) -> dict:
    """Relit un fichier archivé (utile pour rejouer la transformation seule)."""
    chemin = chemin_archive(ville, ds)
    with open(chemin, "r", encoding="utf-8") as f:
        return json.load(f)
