"""
TP 5 — Client API Open-Meteo.

Module isolé : aucune logique Airflow ici, uniquement l'appel HTTP.
Facilite les tests unitaires et le remplacement de l'API si besoin.
"""

import logging

import requests

from tp5.config import API_URL, API_TIMEOUT_SECONDS, CHAMPS_DAILY

logger = logging.getLogger(__name__)


def recuperer_meteo(ville: str, latitude: float, longitude: float, forecast_days: int) -> dict:
    """
    Appelle l'API Open-Meteo pour une ville donnée.

    Lève une exception (requests.HTTPError / Timeout) en cas d'échec —
    Airflow se charge des retries grâce aux paramètres de la tâche.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ",".join(CHAMPS_DAILY),
        "timezone": "Europe/Paris",
        "forecast_days": forecast_days,
    }

    logger.info("Appel Open-Meteo pour %s (lat=%s, lon=%s, jours=%s)",
                 ville, latitude, longitude, forecast_days)

    response = requests.get(API_URL, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()

    payload = response.json()
    logger.info("[%s] réponse OK — %s octets", ville, len(response.content))
    return payload
