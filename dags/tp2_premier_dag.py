from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def extraire_donnees():
    print("Extraction des données source...")
    print("Données extraites : {'ventes': 1500, 'clients': 42}")


def transformer_donnees():
    print("Transformation en cours : nettoyage + agrégation...")
    print("Résultat transformé : {'total_ventes': 1500, 'moyenne': 35.7}")


def charger_donnees():
    print("Chargement vers la destination finale...")
    print("Chargement terminé avec succès.")


with DAG(
    dag_id="tp2_pipeline_simple",
    description="Pipeline ETL simple : extraction → transformation → chargement",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tp2", "etl"],
) as dag:

    extraction = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire_donnees,
    )

    transformation = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer_donnees,
    )

    chargement = PythonOperator(
        task_id="charger_donnees",
        python_callable=charger_donnees,
    )

    # Dépendances explicites : ETL séquentiel
    extraction >> transformation >> chargement
