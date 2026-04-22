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

## PStructure du projet
src/
    extract.py
    transform.py
    load.py
    main.py
data/

## How to run
```bash
python src/main.py

```

## Features
- Modular pipeline (ETL)
- Clean project structure
- Easy to extend

## Next Improvements
Add logging
Add error handling
Add scheduling (cron / Airflow)