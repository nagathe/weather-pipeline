# main.py  (dans /src)
from extract   import extract
from transform import transform
from load      import load


def run_pipeline(cities: list[str]) -> None:
    print("\n" + "=" * 50)
    print("  PIPELINE METEO — démarrage")
    print("=" * 50)

    records = extract(cities)
    if not records:
        print("[ERROR] Aucune donnée extraite — pipeline arrete.")
        return

    df_clean = transform(records)
    if df_clean.empty:
        print("[ERROR] DataFrame vide apres transform — pipeline arrete.")
        return

    load(df_clean)

    print("\n  PIPELINE — termine avec succes")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    #CITIES = ["Paris", "London", "Berlin", "Madrid", "Tokyo"]
    CITIES = ["Paris", "London", "Berlin", "Madrid", "Tokyo", "New York", "Dubai", "Sydney", "São Paulo", "Cairo"]
    # comment faire un appel à deux séries de villes ?
    run_pipeline(CITIES)
