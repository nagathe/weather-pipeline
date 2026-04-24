# Weather Data Pipeline

## Overview Projet
*Projet d'implémentation d'un pipeline (simple) ETL :*
- extraction de données météo OpenWeatherMap via API
- transformation des données
- chargement des données en BDD
- visualisation des données (statique ou interactive)

##### Objectif : analyser les évolutions de la météo dans le temps
*Construire un historique exploitable des conditions météo :*
- variation de température par jour
- évolution de l'humidité
- fréquence des conditions météo (pluie, soleil)

*(objectif principal : s'entraîner à coder un ETL en Python)*

*Dataset utilisable pour :*
- time series analysis
- reporting
- feature engineering

---

## Stack technique
- Python
- Pandas
- Requests
- SQLite
- Matplotlib / Seaborn
- Streamlit
- Docker
- Apache Airflow

---

## Structure du projet
```text
│
├── dags/
│   └── weather_pipeline_dag.py         # DAG Airflow (ETL + visualisation)
│
├── data/
│   ├── weather.db                      # Base SQLite (générée auto)
│   ├── weather_YYYYMMDD_HHMMSS.csv     # Export CSV (généré auto)
│   └── dashboard.png                   # Dashboard PNG (généré auto)
│
├── src/
│   ├── extract.py          # Extraction via API OpenWeatherMap
│   ├── transform.py        # Nettoyage et transformation
│   ├── load.py             # Stockage CSV + SQLite
│   ├── visualize.py        # Dashboard statique (matplotlib)
│   ├── app_streamlit.py    # Dashboard interactif (Streamlit)
│   ├── config.py           # Configuration (villes, paramètres)
│   └── main.py             # Orchestrateur principal
│
├── Dockerfile                  # Image Docker pipeline ETL
├── Dockerfile.streamlit        # Image Docker Streamlit
├── docker-compose.yaml         # Orchestration Airflow + Streamlit
├── .env                        # API_KEY=... (non versionné)
├── requirements.txt
└── README.md
```

---

## Installation locale

```bash
git clone https://github.com/YOUR_USERNAME/weather-pipeline.git
cd weather-pipeline

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

## How to run (local)

1. Pipeline ETL (extract → transform → load) → Récupère les données météo, les transforme et les stocke en CSV + SQLite.
```bash
python src/main.py
```

2. Visualisation statique → Génère un dashboard matplotlib et le sauvegarde dans data/dashboard.png.
```bash
python src/visualize.py
```

3. Dashboard interactif → Ouvre un dashboard interactif dans le navigateur sur http://localhost:8501.
```bash
streamlit run src/app_streamlit.py
```

---

## Installation & Run avec Docker

### Pipeline seul
```bash
# Sans persistance des données
docker run --rm --env-file .env nagathe/weather-pipeline:1.0

# Avec persistance des données
docker run --rm --env-file .env -v $(pwd)/data:/app/data nagathe/weather-pipeline:1.0
```

### Stack complète (Airflow + Streamlit)
```bash
# Initialisation (première fois uniquement)
docker compose up airflow-init

# Lancer tous les services
docker compose up -d
```

- **Airflow** : http://localhost:8080 (login: airflow / airflow)
- **Streamlit** : http://localhost:8501

Le DAG `weather_pipeline` orchestre automatiquement :