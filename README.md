# Weather Data Pipeline

## Overview Projet
*Projet d'implémentation d'un pipeline (simple) ETL :*
- extraction de données météo OpenWeatherMap via API
- transformation des données
- charger les données en BDD

##### Objectif : analyser les évolutions de la météo dans le temps
*Construire un historique exploitable des conditions météo :*
- variation de température par jour
- évolution de l'humidité
- fréquence des conditions météo (pluie, soleil)
(objectif premier : s'entrainer à coder un ETL en python)

*Data set pour :*
  - time series analysis
  - reporting
  - feature engineerign

## Stack technique
- Python
- Pandas
- Requests

## Structure du projet
```
│
├── data/
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── test_api.py
│   ├── config.py
│   └── main.py
│
├── requirements.txt
└── README.md
```

## How to run
```bash
git clone https://github.com/YOUR_USERNAME/weather-pipeline.git
cd weather-pipeline

pip install -r requirements.txt

python src/main.py
```

## Features
- Modular ETL pipeline (extract / transform / load)
- Separation of concerns between pipeline steps
- Easily extensible architecture
- Simple and readable codebase

## Next Improvements
- Add logging
- Add unit tests
- Dockerize the pipeline
- Schedule execution (cron / Airflow)
- Store data in a database (PostgreSQL, BigQuery...)

## Requetes visées
1. Températures dans le temps : moyenne/joinr, min/max, tendances hausse/baisse
2. Conditions météo : combien de jours de pluie ? de jours ensoleillés ?
3. Humidité : évolution journalière, corrélation avec la météo
4. Mutli-villes : Paris vs Lyon, différences de climat ?
