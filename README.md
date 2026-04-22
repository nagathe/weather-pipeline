# Weather Data Pipeline

## Overview Projet
Projet d'implémentation d'un pipeline (simple) ETL :
- extraction de données météo OpenWeatherMap via API
- transformation des données
- charger les données en BDD

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
│   └── main.py
│
├── config.py
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