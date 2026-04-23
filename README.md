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

---

## Structure du projet
```text
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
├── .env                    # API_KEY=... (non versionné)
├── requirements.txt
└── README.md
```

---

## Installation

```
git clone https://github.com/YOUR_USERNAME/weather-pipeline.git
cd weather-pipeline

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```


## How to run
1. Pipeline ETL (extract → transform → load) → Récupère les données météo, les transforme et les stocke en CSV + SQLite.
```
python src/main.py
```

2. Visualisation statique → Génère un dashboard matplotlib et le sauvegarde dans data/dashboard.png.
```
python src/visualize.py
```

3. Dashboard interactif → Ouvre un dashboard interactif dans le navigateur sur http://localhost:8501.
```
streamlit run src/app_streamlit.py
```

## Features
- Modular ETL pipeline (extract / transform / load)
- Logging structuré à chaque étape
- Separation of concerns between pipeline steps
- Stockage CSV + SQLite avec batch_id
- Dashboard statique exportable (PNG)
- Dashboard interactif avec filtres (Streamlit)
- Easily extensible architecture

---

## Requêtes visées
1. **Températures dans le temps** : moyenne/jour, min/max, tendances
2. **Conditions météo** : combien de jours de pluie ? de soleil ?
3. **Humidité** : évolution journalière, corrélation avec la météo
4. **Multi-villes** : Paris vs London, différences de climat ?

---

## Next Improvements
- [ ] Ajouter des tests unitaires
- [ ] Dockeriser le pipeline
- [ ] Planifier l'exécution (cron / Airflow)
- [ ] Migrer vers PostgreSQL ou BigQuery
- [ ] Déployer le dashboard Streamlit en ligne

## Docker
docker run --rm --env-file .env weather-pipeline:1.0
-- attention => données du pipeline sont dans le docker donc dès qu'on l'arrête, on perd tout
-- pour ne pas perdre les données : docker run --rm --env-file .env -v $(pwd)/data:/app/data weather-pipeline:1.0