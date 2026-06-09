"""
TP 2B — Pipeline complet : Open-Meteo → transformation → PostgreSQL
Tâches :
  1. recuperer_donnees_brutes   : appel API Open-Meteo, stocke JSON brut
  2. transformer_donnees        : extrait champs utiles, structure lignes cible
  3. charger_en_base            : INSERT dans meteo_quotidienne (ON CONFLICT UPDATE)
  4. ecrire_suivi_ingestion     : trace l'exécution dans suivi_ingestion

Paramétrage DAG (dag_run.conf) :
  {
    "villes": ["Paris", "Lyon", "Marseille"],
    "forecast_days": 7
  }
"""

import json
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


# ── Configuration par défaut (surchargeable via dag_run.conf) ──────────────

VILLES_DEFAUT = {
    "Paris":     {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon":      {"latitude": 45.7640, "longitude": 4.8357},
    "Bordeaux":  {"latitude": 44.8378, "longitude": -0.5792},
}

TOUTES_VILLES = {
    **VILLES_DEFAUT,
    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
    "Toulouse":  {"latitude": 43.6047, "longitude": 1.4442},
    "Nantes":    {"latitude": 47.2184, "longitude": -1.5536},
}

CHAMPS_DAILY = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]

DB_CONFIG = {
    "host":     "postgres",   # nom du service docker-compose
    "port":     5432,
    "dbname":   "airflow",
    "user":     "airflow",
    "password": "airflow",
}


# ── Helpers ────────────────────────────────────────────────────────────────

def get_villes_config(context) -> dict:
    """Retourne les villes depuis dag_run.conf si fourni, sinon défaut."""
    conf = context["dag_run"].conf or {}
    noms = conf.get("villes", list(VILLES_DEFAUT.keys()))
    return {v: TOUTES_VILLES[v] for v in noms if v in TOUTES_VILLES}


def get_forecast_days(context) -> int:
    conf = context["dag_run"].conf or {}
    return int(conf.get("forecast_days", 7))


# ── Tâche 1 : Récupération brute ───────────────────────────────────────────

def recuperer_donnees_brutes(**context):
    villes = get_villes_config(context)
    forecast_days = get_forecast_days(context)
    resultats_bruts = {}

    for ville, coords in villes.items():
        params = {
            "latitude":     coords["latitude"],
            "longitude":    coords["longitude"],
            "daily":        ",".join(CHAMPS_DAILY),
            "timezone":     "Europe/Paris",
            "forecast_days": forecast_days,
        }
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        resultats_bruts[ville] = response.json()
        print(f"[{ville}] OK — {len(response.content)} octets reçus")

    context["ti"].xcom_push(key="donnees_brutes", value=resultats_bruts)


# ── Tâche 2 : Transformation ───────────────────────────────────────────────

def transformer_donnees(**context):
    donnees_brutes = context["ti"].xcom_pull(
        key="donnees_brutes", task_ids="recuperer_donnees_brutes"
    )
    lignes = []

    for ville, payload in donnees_brutes.items():
        daily = payload.get("daily", {})
        dates       = daily.get("time", [])
        temp_max    = daily.get("temperature_2m_max", [])
        temp_min    = daily.get("temperature_2m_min", [])
        precipi     = daily.get("precipitation_sum", [])
        vent        = daily.get("windspeed_10m_max", [])

        for i, date in enumerate(dates):
            lignes.append({
                "ville":            ville,
                "date":             date,
                "temperature_max":  temp_max[i]  if i < len(temp_max)  else None,
                "temperature_min":  temp_min[i]  if i < len(temp_min)  else None,
                "precipitation_mm": precipi[i]   if i < len(precipi)   else None,
                "vent_max_kmh":     vent[i]       if i < len(vent)      else None,
            })

    print(f"{len(lignes)} lignes transformées")
    print("Aperçu (3 premières lignes) :")
    for l in lignes[:3]:
        print(json.dumps(l, ensure_ascii=False))

    context["ti"].xcom_push(key="lignes_transformees", value=lignes)


# ── Tâche 3 : Chargement PostgreSQL ───────────────────────────────────────

def charger_en_base(**context):
    lignes = context["ti"].xcom_pull(
        key="lignes_transformees", task_ids="transformer_donnees"
    )

    sql = """
        INSERT INTO meteo_quotidienne
            (ville, date, temperature_max, temperature_min, precipitation_mm, vent_max_kmh)
        VALUES
            (%(ville)s, %(date)s, %(temperature_max)s, %(temperature_min)s,
             %(precipitation_mm)s, %(vent_max_kmh)s)
        ON CONFLICT (ville, date)
        DO UPDATE SET
            temperature_max  = EXCLUDED.temperature_max,
            temperature_min  = EXCLUDED.temperature_min,
            precipitation_mm = EXCLUDED.precipitation_mm,
            vent_max_kmh     = EXCLUDED.vent_max_kmh,
            ingere_le        = NOW();
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, lignes)
        print(f"{len(lignes)} lignes chargées dans meteo_quotidienne")
    finally:
        conn.close()

    context["ti"].xcom_push(key="nb_lignes_chargees", value=len(lignes))


# ── Tâche 4 : Suivi d'ingestion ────────────────────────────────────────────

def ecrire_suivi_ingestion(**context):
    nb_lignes = context["ti"].xcom_pull(
        key="nb_lignes_chargees", task_ids="charger_en_base"
    )
    villes = list(get_villes_config(context).keys())

    sql = """
        INSERT INTO suivi_ingestion (dag_id, run_id, villes, nb_lignes, statut, message)
        VALUES (%(dag_id)s, %(run_id)s, %(villes)s, %(nb_lignes)s, %(statut)s, %(message)s);
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "dag_id":   context["dag"].dag_id,
                    "run_id":   context["run_id"],
                    "villes":   json.dumps(villes),
                    "nb_lignes": nb_lignes,
                    "statut":   "succes",
                    "message":  f"Ingestion terminée — {nb_lignes} lignes pour {villes}",
                })
        print(f"Suivi écrit — run_id={context['run_id']}, nb_lignes={nb_lignes}")
    finally:
        conn.close()


# ── DAG ────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="tp2b_pipeline_meteo_postgres",
    description="Pipeline complet : Open-Meteo → transformation → PostgreSQL + suivi",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2b", "meteo", "postgres", "etl"],
    params={
        "villes":        ["Paris", "Lyon", "Bordeaux"],
        "forecast_days": 7,
    },
) as dag:

    t1_recuperation = PythonOperator(
        task_id="recuperer_donnees_brutes",
        python_callable=recuperer_donnees_brutes,
    )

    t2_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    t3_chargement = PythonOperator(
        task_id="charger_en_base",
        python_callable=charger_en_base,
    )

    t4_suivi = PythonOperator(
        task_id="ecrire_suivi_ingestion",
        python_callable=ecrire_suivi_ingestion,
    )

    # Séparation claire : récupération → transformation → chargement → traçabilité
    t1_recuperation >> t2_transformation >> t3_chargement >> t4_suivi
