# TP 5 — Industrialisation d'un pipeline Airflow Open-Meteo

## 1. Description du pipeline

Pipeline Airflow `tp5_pipeline_meteo_open_meteo` qui, pour une liste de
villes configurable :

1. récupère les prévisions météo via l'API Open-Meteo ;
2. archive la réponse brute (JSON) sur disque, sans modification ;
3. transforme les données en lignes exploitables ;
4. contrôle la qualité de chaque ligne ;
5. **bifurque** :
   - aucune anomalie → charge les données dans PostgreSQL ;
   - au moins une anomalie → bloque le chargement final et trace
     l'anomalie ;
6. écrit dans tous les cas une ligne de suivi d'ingestion (succès ou
   anomalie) dans PostgreSQL.

## 2. Schéma du workflow

```
extraire_donnees
      │
      ▼
archiver_donnees_brutes
      │
      ▼
transformer_donnees
      │
      ▼
controler_qualite_donnees
      │
      ▼
decider_chargement (BranchPythonOperator)
      │
      ├── (qualité OK)  ──► charger_donnees_valides ──┐
      │                                               ├──► ecrire_suivi_ingestion
      └── (anomalie)    ──► tracer_anomalie_qualite ──┘
```

`ecrire_suivi_ingestion` utilise `trigger_rule="none_failed_min_one_success"`
pour s'exécuter quelle que soit la branche empruntée (l'autre branche est
"skipped", ce qui est normal avec `BranchPythonOperator`).

## 3. Structure du projet

```
TP-5/
├── README.md
├── sql/
│   └── init_tables.sql        # création des 3 tables PostgreSQL
└── dags/
    ├── tp5_pipeline_meteo.py   # DAG principal (orchestration uniquement)
    └── tp5/                    # modules Python métier (package)
        ├── __init__.py
        ├── config.py           # constantes : villes, seuils, connexion DB
        ├── api_client.py        # appel API Open-Meteo
        ├── archive.py            # archivage JSON brut
        ├── transform.py          # transformation -> lignes plates
        ├── quality.py             # règles qualité + simulation d'anomalie
        ├── load.py                # chargement PostgreSQL (idempotent)
        └── tracking.py            # écriture suivi_ingestion (idempotent)
```

**Déploiement** : copier `dags/tp5_pipeline_meteo.py` et le dossier
`dags/tp5/` dans le dossier `dags/` monté par docker-compose. Exécuter
`sql/init_tables.sql` sur la base `airflow` avant le premier run.

```powershell
copy "TP-5\dags\tp5_pipeline_meteo.py" "TP-Airflow\dags\"
copy -r "TP-5\dags\tp5" "TP-Airflow\dags\tp5"
docker-compose exec postgres psql -U airflow -d airflow -f /dev/stdin < "TP-5\sql\init_tables.sql"
```

Le `docker-compose.yml` racine a été mis à jour pour :
- monter `./data:/opt/airflow/data` (archivage des JSON bruts) ;
- installer `psycopg2-binary` et `requests` via
  `_PIP_ADDITIONAL_REQUIREMENTS`.

Après modification du compose, redémarrer :
```powershell
docker-compose up -d
```

## 4. Variables et connexions Airflow utilisées

Le TP utilise une configuration **centralisée dans `tp5/config.py`**
(via variables d'environnement avec valeurs par défaut), plutôt que des
Airflow Variables/Connections, pour rester simple à déployer :

| Nom (env var) | Rôle | Valeur par défaut |
|---|---|---|
| `TP5_DB_HOST` | hôte PostgreSQL | `postgres` |
| `TP5_DB_PORT` | port PostgreSQL | `5432` |
| `TP5_DB_NAME` | base | `airflow` |
| `TP5_DB_USER` | utilisateur | `airflow` |
| `TP5_DB_PASSWORD` | mot de passe | `airflow` |
| `TP5_ARCHIVE_DIR` | dossier d'archivage JSON | `/opt/airflow/data/raw` |

> Limite assumée : en environnement de production, ces paramètres
> seraient déplacés dans une **Airflow Connection** `postgres_meteo`
> (utilisée via `PostgresHook`) et des **Airflow Variables** pour la
> liste de villes par défaut. Voir section "Limites".

**Paramétrage par run** (`dag_run.conf`, via Trigger DAG w/ config) :

```json
{
  "villes": ["Paris", "Lyon", "Bordeaux"],
  "forecast_days": 7,
  "simuler_anomalie": false
}
```

## 5. Description des tâches du DAG

| Tâche | Type | Rôle |
|---|---|---|
| `extraire_donnees` | PythonOperator | Appelle Open-Meteo pour chaque ville. `retries=3`, `retry_delay=30s`, `execution_timeout=2min`. |
| `archiver_donnees_brutes` | PythonOperator | Écrit le JSON brut sur disque (`data/raw/<ds>/<ds>_<ville>.json`), sans transformation. |
| `transformer_donnees` | PythonOperator | Extrait les champs utiles (température, précipitations, vent) et structure les lignes. Injecte une anomalie si `simuler_anomalie=true`. |
| `controler_qualite_donnees` | PythonOperator | Applique les règles qualité (cf. §7), sépare lignes valides / anomalies. |
| `decider_chargement` | BranchPythonOperator | Choisit la branche `charger_donnees_valides` ou `tracer_anomalie_qualite`. |
| `charger_donnees_valides` | PythonOperator | INSERT/UPDATE dans `meteo_quotidienne` (cas nominal). |
| `tracer_anomalie_qualite` | PythonOperator | INSERT/UPDATE dans `meteo_anomalies`, **aucun chargement** dans `meteo_quotidienne`. |
| `ecrire_suivi_ingestion` | PythonOperator | INSERT/UPDATE dans `suivi_ingestion`, quel que soit le résultat. `trigger_rule=none_failed_min_one_success`. |

## 6. Stratégie de robustesse

- **Retries / retry_delay / timeout** : la tâche `extraire_donnees` (point
  le plus exposé aux pannes réseau/API) a `retries=3`,
  `retry_delay=timedelta(seconds=30)`, `execution_timeout=timedelta(minutes=2)`.
  Le reste du DAG hérite de `default_args` (`retries=3`, `retry_delay=30s`,
  `execution_timeout=5min`).
- **Gestion des erreurs** : `requests.raise_for_status()` lève une
  exception sur toute erreur HTTP → la tâche échoue proprement et Airflow
  retente selon la politique ci-dessus, puis passe la tâche en `failed`
  si tous les essais échouent (le DAG ne charge alors rien).
- **Logs applicatifs** : chaque module utilise `logging` (visible dans
  les logs de tâche Airflow) — volumes téléchargés, nombre de lignes
  transformées, anomalies détectées, lignes chargées, etc.

## 7. Stratégie d'idempotence

Une relance (DAG entier ou tâche individuelle) avec le même `run_id` /
`execution_date` ne crée **aucun doublon** :

- **Archivage** : nom de fichier basé sur `ds` + ville → un rerun écrase
  le même fichier.
- **`meteo_quotidienne`** : `UNIQUE (ville, date)` +
  `ON CONFLICT ... DO UPDATE` → une ligne par (ville, date), mise à jour
  en place.
- **`meteo_anomalies`** : `UNIQUE (ville, date, champ, run_id)` +
  `ON CONFLICT ... DO UPDATE`.
- **`suivi_ingestion`** : `UNIQUE (dag_id, run_id)` +
  `ON CONFLICT ... DO UPDATE` → une seule ligne de suivi par exécution,
  même après plusieurs relances.

## 8. Contrôles qualité mis en place (`tp5/quality.py`)

Pour chaque ligne (ville, date) :

1. champs obligatoires non `NULL` (`ville`, `date`, `temperature_max`,
   `temperature_min`, `precipitation_mm`, `vent_max_kmh`) ;
2. `temperature_max` et `temperature_min` ∈ `[-50, 60]` °C ;
3. `temperature_min <= temperature_max` ;
4. `precipitation_mm` ∈ `[0, 500]` mm ;
5. `vent_max_kmh` ∈ `[0, 250]` km/h.

Toute violation → entrée dans `meteo_anomalies` (champ, valeur, règle
violée).

## 9. Règle de branchement conditionnel

```python
if anomalies:           # au moins une anomalie détectée sur le batch
    return "tracer_anomalie_qualite"
else:
    return "charger_donnees_valides"
```

→ approche "tout ou rien" par run : si une seule ligne du batch est en
anomalie, **aucune** ligne n'est chargée dans `meteo_quotidienne` pour ce
run, et toutes les anomalies sont tracées. Ce choix simplifie la
démonstration et évite les chargements partiels incohérents (cf. §13).

## 10. Logs produits

- **Logs Airflow standard** (par tâche, dans l'UI → Grid → tâche → Log) :
  appels API (`[Ville] OK — N octets`), chemins d'archivage, nombre de
  lignes transformées, résultat du contrôle qualité
  (`X lignes valides / Y anomalies`), warnings d'anomalies détaillées,
  nombre de lignes chargées ou anomalie tracée.
- **Logs applicatifs Python** : tous les modules `tp5/*` utilisent
  `logging.getLogger(__name__)`, capturés automatiquement par Airflow.

## 11. Description des tables PostgreSQL

### `meteo_quotidienne` — données validées
| Colonne | Type | Description |
|---|---|---|
| `ville`, `date` | VARCHAR / DATE | clé fonctionnelle (`UNIQUE`) |
| `temperature_max/min` | NUMERIC | °C |
| `precipitation_mm` | NUMERIC | mm |
| `vent_max_kmh` | NUMERIC | km/h |
| `run_id` | VARCHAR | run Airflow ayant écrit/maj la ligne |
| `ingere_le` | TIMESTAMP | horodatage de chargement |

### `meteo_anomalies` — anomalies détectées (non chargées)
| Colonne | Description |
|---|---|
| `ville`, `date`, `champ` | localisation de l'anomalie |
| `valeur` | valeur fautive (texte) |
| `regle_violee` | règle qualité violée |
| `run_id` | run ayant détecté l'anomalie |

### `suivi_ingestion` — une ligne par run
| Colonne | Description |
|---|---|
| `dag_id`, `run_id` | identifiants du run (`UNIQUE`) |
| `villes` | liste JSON des villes traitées |
| `nb_lignes_total` / `nb_lignes_valides` / `nb_anomalies` | compteurs |
| `statut` | `succes` \| `anomalie_qualite` \| `erreur` |
| `message` | résumé lisible |

## 12. Preuves d'exécution

> À compléter avec captures d'écran / exports lors de l'exécution réelle.

### a) Cas nominal
- Trigger sans config (ou `simuler_anomalie: false`).
- DAG entièrement vert, branche `charger_donnees_valides` exécutée,
  `tracer_anomalie_qualite` en `skipped`.
- Vérifier :
  ```sql
  SELECT * FROM meteo_quotidienne ORDER BY date;
  SELECT * FROM suivi_ingestion ORDER BY execute_le DESC LIMIT 1;
  -- statut = 'succes'
  ```

### b) Cas d'anomalie qualité
- Trigger avec config :
  ```json
  { "simuler_anomalie": true }
  ```
- Branche `tracer_anomalie_qualite` exécutée, `charger_donnees_valides`
  en `skipped`.
- Vérifier :
  ```sql
  SELECT * FROM meteo_anomalies ORDER BY detecte_le DESC;
  SELECT * FROM suivi_ingestion ORDER BY execute_le DESC LIMIT 1;
  -- statut = 'anomalie_qualite', nb_lignes_valides = 0
  ```
- Confirmer qu'**aucune** nouvelle ligne n'apparaît dans
  `meteo_quotidienne` pour ce run.

### c) Cas de relance (idempotence)
- Relancer le run nominal (bouton "Clear" sur le DAG run, ou re-trigger
  avec la même `execution_date` / mêmes villes).
- Vérifier :
  ```sql
  SELECT ville, date, COUNT(*) FROM meteo_quotidienne
  GROUP BY ville, date HAVING COUNT(*) > 1;
  -- doit retourner 0 ligne (pas de doublon)

  SELECT COUNT(*) FROM suivi_ingestion WHERE run_id = '<run_id>';
  -- doit retourner 1
  ```
- `ingere_le` / `execute_le` mis à jour mais nombre de lignes inchangé.

## 13. Limites éventuelles du travail rendu

- Connexion PostgreSQL en dur dans `config.py` (via env vars) plutôt
  qu'une Airflow Connection + `PostgresHook` — choix pour limiter la
  configuration manuelle dans l'UI, mais moins idiomatique Airflow.
- Branchement "tout ou rien" par run : si une seule ville/jour est en
  anomalie, **tout le batch** est bloqué plutôt que de charger
  partiellement les lignes valides. Une évolution possible serait de
  charger les lignes valides ET de tracer les anomalies dans le même run
  (deux opérations non exclusives).
- Pas de `SLA` ni d'alerting (email/Slack) configuré sur échec de tâche.
- Le client API ne gère pas la pagination/rate-limiting avancé
  d'Open-Meteo (non nécessaire pour le volume du TP).
