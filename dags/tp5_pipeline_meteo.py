"""
TP 5 — Pipeline industrialisé Open-Meteo.

Récupération -> Archivage -> Transformation -> Contrôle qualité ->
branchement conditionnel -> Chargement PostgreSQL OU traçage anomalie ->
Suivi d'ingestion.

Paramétrage (Trigger DAG w/ config) :
    {
      "villes": ["Paris", "Lyon", "Bordeaux"],
      "forecast_days": 7,
      "simuler_anomalie": false
    }

Voir README.md pour le détail des tâches, des tables et des preuves
attendues.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

from tp5.api_client import recuperer_meteo
from tp5.archive import archiver_payload
from tp5.config import VILLES_DISPONIBLES, VILLES_PAR_DEFAUT, FORECAST_DAYS_PAR_DEFAUT
from tp5.load import charger_lignes_valides, tracer_anomalies
from tp5.quality import controler_qualite, injecter_anomalie
from tp5.tracking import ecrire_suivi
from tp5.transform import transformer_payload

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────
# Helpers de configuration de run
# ─────────────────────────────────────────────────────────────────────────

def _get_conf(context: dict) -> dict:
    return context["dag_run"].conf or {}


def _get_villes(context: dict) -> dict:
    noms = _get_conf(context).get("villes", VILLES_PAR_DEFAUT)
    return {v: VILLES_DISPONIBLES[v] for v in noms if v in VILLES_DISPONIBLES}


# ─────────────────────────────────────────────────────────────────────────
# Tâche 1 : Extraction (API Open-Meteo)
# ─────────────────────────────────────────────────────────────────────────

def extraire_donnees(**context):
    """
    Appelle l'API Open-Meteo pour chaque ville configurée.
    Les retries / retry_delay / timeout sont gérés au niveau de la tâche
    Airflow (cf. PythonOperator ci-dessous).
    """
    villes = _get_villes(context)
    forecast_days = _get_conf(context).get("forecast_days", FORECAST_DAYS_PAR_DEFAUT)

    payloads = {}
    for ville, coords in villes.items():
        payloads[ville] = recuperer_meteo(
            ville, coords["latitude"], coords["longitude"], forecast_days
        )

    context["ti"].xcom_push(key="payloads_bruts", value=payloads)
    logger.info("Extraction terminée pour : %s", list(payloads.keys()))


# ─────────────────────────────────────────────────────────────────────────
# Tâche 2 : Archivage des données brutes
# ─────────────────────────────────────────────────────────────────────────

def archiver_donnees_brutes(**context):
    payloads = context["ti"].xcom_pull(key="payloads_bruts", task_ids="extraire_donnees")
    ds = context["ds"]

    chemins = {}
    for ville, payload in payloads.items():
        chemins[ville] = archiver_payload(ville, ds, payload)

    context["ti"].xcom_push(key="chemins_archives", value=chemins)
    logger.info("Archivage terminé : %s", chemins)


# ─────────────────────────────────────────────────────────────────────────
# Tâche 3 : Transformation
# ─────────────────────────────────────────────────────────────────────────

def transformer_donnees(**context):
    payloads = context["ti"].xcom_pull(key="payloads_bruts", task_ids="extraire_donnees")

    lignes = []
    for ville, payload in payloads.items():
        lignes.extend(transformer_payload(ville, payload))

    # Simulation d'anomalie qualité (cas de démonstration du TP)
    if _get_conf(context).get("simuler_anomalie", False):
        lignes = injecter_anomalie(lignes)

    context["ti"].xcom_push(key="lignes_transformees", value=lignes)
    logger.info("Transformation terminée : %s lignes", len(lignes))


# ─────────────────────────────────────────────────────────────────────────
# Tâche 4 : Contrôle qualité
# ─────────────────────────────────────────────────────────────────────────

def controler_qualite_donnees(**context):
    lignes = context["ti"].xcom_pull(key="lignes_transformees", task_ids="transformer_donnees")

    lignes_valides, anomalies = controler_qualite(lignes)

    context["ti"].xcom_push(key="lignes_valides", value=lignes_valides)
    context["ti"].xcom_push(key="anomalies", value=anomalies)
    context["ti"].xcom_push(key="nb_lignes_total", value=len(lignes))


# ─────────────────────────────────────────────────────────────────────────
# Tâche 5 : Branchement conditionnel
# ─────────────────────────────────────────────────────────────────────────

def decider_chargement(**context):
    """
    Règle de branchement :
      - s'il existe AU MOINS UNE anomalie qualité -> on NE charge PAS les
        données dans meteo_quotidienne, on trace uniquement l'anomalie ;
      - sinon -> chargement normal des données validées.
    """
    anomalies = context["ti"].xcom_pull(key="anomalies", task_ids="controler_qualite_donnees")

    if anomalies:
        logger.warning("%s anomalie(s) détectée(s) -> branche tracer_anomalie_qualite", len(anomalies))
        return "tracer_anomalie_qualite"

    logger.info("Aucune anomalie -> branche charger_donnees_valides")
    return "charger_donnees_valides"


# ─────────────────────────────────────────────────────────────────────────
# Tâche 6a : Chargement (cas nominal)
# ─────────────────────────────────────────────────────────────────────────

def charger_donnees_valides(**context):
    lignes_valides = context["ti"].xcom_pull(key="lignes_valides", task_ids="controler_qualite_donnees")
    run_id = context["run_id"]

    nb = charger_lignes_valides(lignes_valides, run_id)
    context["ti"].xcom_push(key="nb_charge", value=nb)


# ─────────────────────────────────────────────────────────────────────────
# Tâche 6b : Traçage de l'anomalie (cas anomalie qualité)
# ─────────────────────────────────────────────────────────────────────────

def tracer_anomalie_qualite(**context):
    anomalies = context["ti"].xcom_pull(key="anomalies", task_ids="controler_qualite_donnees")
    run_id = context["run_id"]

    nb = tracer_anomalies(anomalies, run_id)
    context["ti"].xcom_push(key="nb_charge", value=0)
    logger.warning("Chargement final BLOQUÉ — %s anomalie(s) tracée(s) dans meteo_anomalies", nb)


# ─────────────────────────────────────────────────────────────────────────
# Tâche 7 : Suivi d'ingestion (jointure des deux branches)
# ─────────────────────────────────────────────────────────────────────────

def ecrire_suivi_ingestion(**context):
    ti = context["ti"]
    nb_total = ti.xcom_pull(key="nb_lignes_total", task_ids="controler_qualite_donnees") or 0
    anomalies = ti.xcom_pull(key="anomalies", task_ids="controler_qualite_donnees") or []
    nb_charge = ti.xcom_pull(key="nb_charge", task_ids="charger_donnees_valides")
    if nb_charge is None:
        nb_charge = ti.xcom_pull(key="nb_charge", task_ids="tracer_anomalie_qualite") or 0

    villes = list(_get_villes(context).keys())
    statut = "anomalie_qualite" if anomalies else "succes"
    message = (
        f"{len(anomalies)} anomalie(s) détectée(s) — chargement bloqué"
        if anomalies
        else f"{nb_charge} ligne(s) chargée(s) avec succès"
    )

    ecrire_suivi(
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        villes=villes,
        nb_total=nb_total,
        nb_valides=nb_charge,
        nb_anomalies=len(anomalies),
        statut=statut,
        message=message,
    )


# ─────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "tp5",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="tp5_pipeline_meteo_open_meteo",
    description="Pipeline industrialisé Open-Meteo : extraction, archivage, transformation, "
                 "contrôle qualité, branchement conditionnel, chargement PostgreSQL, suivi",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp5", "meteo", "postgres", "qualite"],
    params={
        "villes": VILLES_PAR_DEFAUT,
        "forecast_days": FORECAST_DAYS_PAR_DEFAUT,
        "simuler_anomalie": False,
    },
) as dag:

    t1_extraction = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire_donnees,
        # retries/timeout dédiés : l'appel API est le point le plus fragile
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )

    t2_archivage = PythonOperator(
        task_id="archiver_donnees_brutes",
        python_callable=archiver_donnees_brutes,
    )

    t3_transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    t4_qualite = PythonOperator(
        task_id="controler_qualite_donnees",
        python_callable=controler_qualite_donnees,
    )

    t5_branchement = BranchPythonOperator(
        task_id="decider_chargement",
        python_callable=decider_chargement,
    )

    t6a_chargement = PythonOperator(
        task_id="charger_donnees_valides",
        python_callable=charger_donnees_valides,
    )

    t6b_anomalie = PythonOperator(
        task_id="tracer_anomalie_qualite",
        python_callable=tracer_anomalie_qualite,
    )

    t7_suivi = PythonOperator(
        task_id="ecrire_suivi_ingestion",
        python_callable=ecrire_suivi_ingestion,
        # s'exécute quelle que soit la branche empruntée (et même si l'une
        # des deux branches a été "skipped" par le branchement)
        trigger_rule="none_failed_min_one_success",
    )

    t1_extraction >> t2_archivage >> t3_transformation >> t4_qualite >> t5_branchement
    t5_branchement >> [t6a_chargement, t6b_anomalie] >> t7_suivi
