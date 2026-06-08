# TP2 — Créer un premier DAG Airflow

Environnement Docker Airflow + un DAG simple à 3 tâches : `weather_daily_pipeline`.

## Arborescence

```
TP2/
├── docker-compose.yaml          # environnement Airflow (LocalExecutor + Postgres)
├── .env                         # AIRFLOW_UID
├── dags/
│   └── weather_daily_pipeline.py
├── logs/                        # logs des tâches (généré)
├── plugins/  config/            # vides
├── preuve_execution.txt         # preuve d'exécution
└── README.md
```

## Lancer l'environnement

```bash
docker compose up -d                # démarre scheduler + webserver
```

Interface web : http://localhost:8080 — identifiants `airflow` / `airflow`.

## Lancer le DAG manuellement

Depuis l'UI : activer le toggle du DAG `weather_daily_pipeline`, puis bouton play (*Trigger DAG*).


## Consulter les logs d'une tâche

UI : *DAG → Grid → clic sur une tâche → onglet Logs*.

Ou sur disque : `logs/dag_id=weather_daily_pipeline/run_id=.../task_id=store_weather/attempt=1.log`

## Le DAG : rôle de chaque tâche

```
fetch_weather  ->  transform_weather  ->  store_weather
```

| Tâche | Rôle (une responsabilité) |
|-------|---------------------------|
| `fetch_weather` | Récupère les mesures météo brutes (appel API simulé). Seul point d'entrée. |
| `transform_weather` | Valide les champs clés et convertit la température en °F. Dépend de `fetch`. |
| `store_weather` | Charge le résultat consolidé (insertion simulée). Tâche terminale, dépend de `transform`. |

**Dépendances** : chaîne linéaire `fetch → transform → store`. Chaque flèche est justifiée — une tâche a besoin de la sortie de la précédente. Pas de tâche fourre-tout, noms explicites en `snake_case` (verbe + complément).

## Comment ça marche (mini explication)

- Le **scheduler** lit le fichier `dags/`, sérialise le DAG en base et crée un **DAG run** au déclenchement.
- Il exécute les **task instances** dans l'ordre des dépendances : `transform` n'attend que la fin de `fetch`, etc.
- Les valeurs passent d'une tâche à l'autre via **XCom** (la valeur retournée par une tâche est l'entrée de la suivante).
- L'**UI** affiche DAGs, runs, états (`success`/`failed`/…) et logs.

## Arrêter

```bash
docker compose down        # arrête les conteneurs
docker compose down -v     # + supprime la base de métadonnées
```
