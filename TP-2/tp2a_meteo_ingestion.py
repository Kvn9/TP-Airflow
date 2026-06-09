"""
TP 2A — Ingestion API météo (Open-Meteo)
DAG : récupération + transformation des données météo pour 3 villes.

Champs retenus (justification ci-dessous) :
  - temperature_2m_max   : temp max journalière — indicateur métier principal
  - temperature_2m_min   : temp min journalière — amplitude thermique
  - precipitation_sum    : cumul pluie (mm)    — utile pour alertes météo
  - windspeed_10m_max    : vent max (km/h)      — sécurité / événements
  - time                 : date du relevé       — clé de la table cible

Champs écartés : données horaires (trop granulaires), radiation, etc.
"""

from datetime import datetime
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


VILLES = {
    "Paris":    {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon":     {"latitude": 45.7640, "longitude": 4.8357},
    "Bordeaux": {"latitude": 44.8378, "longitude": -0.5792},
}

CHAMPS_RETENUS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]


def recuperer_donnees_brutes(**context):
    """
    Appelle l'API Open-Meteo pour chaque ville.
    Stocke la réponse JSON brute dans XCom sans modification.
    """
    resultats_bruts = {}

    for ville, coords in VILLES.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "daily": ",".join(CHAMPS_RETENUS),
            "timezone": "Europe/Paris",
            "forecast_days": 7,
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        resultats_bruts[ville] = response.json()

        print(f"[{ville}] Réponse brute reçue — {len(response.content)} octets")

    context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)
    print(f"Données brutes récupérées pour {list(resultats_bruts.keys())}")


def transformer_donnees(**context):
    """
    Extrait uniquement les champs utiles depuis la réponse brute.
    Produit une liste de lignes prêtes pour insertion en table cible.

    Structure cible :
      { ville, date, temperature_max, temperature_min, precipitation_mm, vent_max_kmh }
    """
    donnees_brutes = context["ti"].xcom_pull(
        key="donnees_brutes", task_ids="recuperer_donnees_brutes"
    )

    lignes_preparees = []

    for ville, payload in donnees_brutes.items():
        daily = payload.get("daily", {})
        dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precipitations = daily.get("precipitation_sum", [])
        vent_max = daily.get("windspeed_10m_max", [])

        for i, date in enumerate(dates):
            ligne = {
                "ville": ville,
                "date": date,
                "temperature_max": temp_max[i] if i < len(temp_max) else None,
                "temperature_min": temp_min[i] if i < len(temp_min) else None,
                "precipitation_mm": precipitations[i] if i < len(precipitations) else None,
                "vent_max_kmh": vent_max[i] if i < len(vent_max) else None,
            }
            lignes_preparees.append(ligne)

    context["ti"].xcom_push(key="donnees_transformees", value=lignes_preparees)

    # Aperçu des données préparées
    print(f"\n=== APERÇU DES DONNÉES PRÉPARÉES ({len(lignes_preparees)} lignes) ===")
    for ligne in lignes_preparees[:6]:
        print(json.dumps(ligne, ensure_ascii=False))
    print("...")


def afficher_resume(**context):
    """
    Affiche un résumé des données transformées (simule la validation avant chargement).
    """
    lignes = context["ti"].xcom_pull(
        key="donnees_transformees", task_ids="transformer_donnees"
    )

    villes_presentes = list({l["ville"] for l in lignes})
    print(f"\n=== RÉSUMÉ PIPELINE ===")
    print(f"Villes traitées : {villes_presentes}")
    print(f"Total lignes prêtes : {len(lignes)}")

    for ville in villes_presentes:
        lignes_ville = [l for l in lignes if l["ville"] == ville]
        temps_max = [l["temperature_max"] for l in lignes_ville if l["temperature_max"] is not None]
        if temps_max:
            print(f"  {ville} — T° max semaine : {max(temps_max)}°C / T° min semaine : {min([l['temperature_min'] for l in lignes_ville if l['temperature_min']])}")


with DAG(
    dag_id="tp2a_ingestion_meteo_open_meteo",
    description="Ingestion API Open-Meteo : récupération et transformation pour 3 villes",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2a", "meteo", "ingestion"],
) as dag:

    recuperation = PythonOperator(
        task_id="recuperer_donnees_brutes",
        python_callable=recuperer_donnees_brutes,
    )

    transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    resume = PythonOperator(
        task_id="afficher_resume",
        python_callable=afficher_resume,
    )

    # Séparation claire : récupération → transformation → validation
    recuperation >> transformation >> resume
