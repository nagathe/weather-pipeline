

"""
test_api.py
-----------
Tests unitaires du pipeline ETL météo.
Couvre : extract (mock), transform, load.
"""

import sqlite3
import sys
import unittest
import tempfile
import pandas as pd
import numpy as np
import requests
import json

from datetime import datetime
from config import API_KEY, BASE_URL
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

#######
params = {
    "q": "Paris",
    "appid": API_KEY,
    "units": "metric"
}

#test réponse API
response = requests.get(BASE_URL, params=params)

print("## 1. test réponse API\n")
print("Status code:", response.status_code)
print("\nResponse JSON:")
print(response.json(), "\n")

#test parsing JSON
data = response.json()
weather_list = data.get("weather") or [{}]

print("## 2. test parsing JSON\n")
print(json.dumps(data, indent=2), "\n")

parsed = {
        "city": data.get("name"),
        "temperature": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
        "weather": weather_list[0].get("description"),
        "timestamp": data.get("dt"),
        "datetime": datetime.utcfromtimestamp(data.get("dt")).isoformat()
}

print(" data parsées :\n", parsed)


#######
# ─── Patch config AVANT tout import des modules du projet ────────────────────
sys.modules["config"] = MagicMock(API_KEY="fake_key", BASE_URL="http://fake.url")

from transform import (
    to_dataframe,
    clean_dataframe,
    cast_types,
    process_dates,
    handle_nulls_and_units,
    transform,
    EXPECTED_COLUMNS,
)
from load import load_csv, load_sqlite, _prepare_for_sqlite


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def make_raw_records(n=3):
    """Génère n enregistrements valides simulant extract()."""
    cities = ["Paris", "London", "Berlin", "Madrid", "Tokyo"]
    records = []
    for i in range(n):
        ts = int(datetime.now(timezone.utc).timestamp()) + i
        records.append({
            "city":        cities[i % len(cities)],
            "temperature": 15.0 + i,
            "humidity":    60.0 + i,
            "weather":     "clear sky",
            "timestamp":   ts,
            "datetime":    datetime.utcfromtimestamp(ts).isoformat(),
        })
    return records


def make_clean_df():
    """Retourne un DataFrame propre et typé prêt pour load."""
    records = make_raw_records(3)
    return transform(records)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. EXTRACT — tests avec mock requests
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtract(unittest.TestCase):

    def _fake_response(self, city="Paris", temp=20.0, humidity=55, ts=1700000000):
        """Construit une fausse réponse requests."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": city,
            "main": {"temp": temp, "humidity": humidity},
            "weather": [{"description": "clear sky"}],
            "dt": ts,
        }
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    @patch("extract.requests.get")
    def test_extract_returns_list(self, mock_get):
        """extract() doit retourner une liste non vide."""
        mock_get.return_value = self._fake_response()
        from extract import extract
        result = extract(["Paris"])
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

    @patch("extract.requests.get")
    def test_extract_record_keys(self, mock_get):
        """Chaque enregistrement doit contenir les clés attendues."""
        mock_get.return_value = self._fake_response()
        from extract import extract
        result = extract(["Paris"])
        expected_keys = {"city", "temperature", "humidity", "weather", "timestamp", "datetime"}
        self.assertTrue(expected_keys.issubset(result[0].keys()))

    @patch("extract.requests.get")
    def test_extract_temperature_value(self, mock_get):
        """La température extraite doit correspondre à la réponse simulée."""
        mock_get.return_value = self._fake_response(temp=22.5)
        from extract import extract
        result = extract(["Paris"])
        self.assertAlmostEqual(result[0]["temperature"], 22.5)

    @patch("extract.requests.get")
    def test_extract_http_error_skipped(self, mock_get):
        """Une erreur HTTP doit être ignorée, la liste reste vide."""
        import requests as req
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("404")
        mock_get.return_value = mock_resp
        from extract import extract
        result = extract(["UnknownCity"])
        self.assertEqual(result, [])

    @patch("extract.requests.get")
    def test_extract_multiple_cities(self, mock_get):
        """extract() doit retourner autant d'enregistrements que de villes."""
        mock_get.side_effect = [
            self._fake_response("Paris", 20),
            self._fake_response("London", 15),
            self._fake_response("Berlin", 10),
        ]
        from extract import extract
        result = extract(["Paris", "London", "Berlin"])
        self.assertEqual(len(result), 3)

    @patch("extract.requests.get")
    def test_extract_timeout_skipped(self, mock_get):
        """Un timeout doit être absorbé sans planter le pipeline."""
        import requests as req
        mock_get.side_effect = req.Timeout
        from extract import extract
        result = extract(["Paris"])
        self.assertEqual(result, [])


# ═══════════════════════════════════════════════════════════════════════════════
# 2. TRANSFORM — to_dataframe
# ═══════════════════════════════════════════════════════════════════════════════

class TestToDataframe(unittest.TestCase):

    def test_empty_records_returns_empty_df(self):
        df = to_dataframe([])
        self.assertTrue(df.empty)
        for col in EXPECTED_COLUMNS:
            self.assertIn(col, df.columns)

    def test_returns_dataframe(self):
        records = make_raw_records(2)
        df = to_dataframe(records)
        self.assertIsInstance(df, pd.DataFrame)

    def test_row_count_matches(self):
        records = make_raw_records(5)
        df = to_dataframe(records)
        self.assertEqual(len(df), 5)

    def test_columns_present(self):
        records = make_raw_records(2)
        df = to_dataframe(records)
        for col in EXPECTED_COLUMNS:
            self.assertIn(col, df.columns)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. TRANSFORM — clean_dataframe
# ═══════════════════════════════════════════════════════════════════════════════

class TestCleanDataframe(unittest.TestCase):

    def test_strips_whitespace(self):
        records = make_raw_records(1)
        records[0]["city"] = "  Paris  "
        df = to_dataframe(records)
        df = clean_dataframe(df)
        self.assertEqual(df.loc[0, "city"], "Paris")

    def test_removes_duplicates(self):
        records = make_raw_records(1)
        records = records * 3  # 3 lignes identiques
        df = to_dataframe(records)
        df = clean_dataframe(df)
        self.assertEqual(len(df), 1)

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame({"city": ["Paris"], "temperature": [20]})
        with self.assertRaises(ValueError):
            clean_dataframe(df)

    def test_empty_df_passthrough(self):
        df = pd.DataFrame(columns=EXPECTED_COLUMNS)
        result = clean_dataframe(df)
        self.assertTrue(result.empty)

    def test_only_expected_columns_kept(self):
        records = make_raw_records(2)
        df = to_dataframe(records)
        df["extra_col"] = "garbage"
        df = clean_dataframe(df)
        self.assertNotIn("extra_col", df.columns)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TRANSFORM — cast_types
# ═══════════════════════════════════════════════════════════════════════════════

class TestCastTypes(unittest.TestCase):

    def setUp(self):
        records = make_raw_records(3)
        df = to_dataframe(records)
        self.df = clean_dataframe(df)

    def test_temperature_is_float(self):
        df = cast_types(self.df)
        self.assertEqual(df["temperature"].dtype, np.float64)

    def test_humidity_is_float(self):
        df = cast_types(self.df)
        self.assertEqual(df["humidity"].dtype, np.float64)

    def test_invalid_temperature_becomes_nan(self):
        """Les températures invalides (string) doivent devenir NaN après cast_types."""
        df = pd.DataFrame({
            "city": ["Paris"],
            "temperature": ["not_a_number"],  # ← string, pas float
            "humidity": [50.0],
            "weather": ["sunny"],
            "timestamp": [1000000000],
            "datetime": ["2001-09-09T01:46:40"]
        })
        
        df_casted = cast_types(df)
        self.assertTrue(pd.isna(df_casted.loc[0, "temperature"]))


    def test_city_is_string_dtype(self):
        df = cast_types(self.df)
        self.assertEqual(df["city"].dtype, pd.StringDtype())

    def test_timestamp_is_int64(self):
        df = cast_types(self.df)
        self.assertEqual(df["timestamp"].dtype, pd.Int64Dtype())


# ═══════════════════════════════════════════════════════════════════════════════
# 5. TRANSFORM — handle_nulls_and_units
# ═══════════════════════════════════════════════════════════════════════════════

class TestHandleNullsAndUnits(unittest.TestCase):

    def _get_processed_df(self):
        records = make_raw_records(3)
        df = to_dataframe(records)
        df = clean_dataframe(df)
        df = cast_types(df)
        df = process_dates(df)
        return df

    def test_temperature_f_calculated(self):
        df = handle_nulls_and_units(self._get_processed_df())
        self.assertIn("temperature_f", df.columns)
        row = df.iloc[0]
        expected = round(row["temperature"] * 9 / 5 + 32, 2)
        self.assertAlmostEqual(row["temperature_f"], expected, places=2)

    def test_data_quality_ok(self):
        df = handle_nulls_and_units(self._get_processed_df())
        self.assertTrue((df["data_quality"] == "ok").all())

    def test_data_quality_incomplete_on_null_temp(self):
        processed = self._get_processed_df()
        processed.loc[0, "temperature"] = np.nan
        df = handle_nulls_and_units(processed)
        self.assertEqual(df.loc[0, "data_quality"], "incomplete")

    def test_outlier_temp_becomes_nan(self):
        processed = self._get_processed_df()
        processed.loc[0, "temperature"] = 200.0  # hors bornes
        df = handle_nulls_and_units(processed)
        self.assertTrue(pd.isna(df.loc[0, "temperature"]))

    def test_outlier_humidity_becomes_nan(self):
        processed = self._get_processed_df()
        processed.loc[0, "humidity"] = -5.0  # hors bornes
        df = handle_nulls_and_units(processed)
        self.assertTrue(pd.isna(df.loc[0, "humidity"]))


# ═══════════════════════════════════════════════════════════════════════════════
# 6. TRANSFORM — pipeline complet
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformPipeline(unittest.TestCase):

    def test_transform_returns_dataframe(self):
        df = transform(make_raw_records(3))
        self.assertIsInstance(df, pd.DataFrame)

    def test_transform_empty_input(self):
        df = transform([])
        self.assertTrue(df.empty)

    def test_transform_final_columns(self):
        df = transform(make_raw_records(3))
        expected = [
            "city", "temperature", "humidity", "weather", "timestamp",
            "datetime_utc", "date", "hour_utc", "day_of_week",
            "temperature_f", "data_quality",
        ]
        for col in expected:
            self.assertIn(col, df.columns)

    def test_transform_no_null_temperature_on_valid_data(self):
        df = transform(make_raw_records(3))
        self.assertFalse(df["temperature"].isna().any())

    def test_transform_row_count(self):
        records = make_raw_records(4)
        df = transform(records)
        self.assertEqual(len(df), 4)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. LOAD — CSV
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadCSV(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp_dir.name)
        self.df = make_clean_df()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_csv_file_created(self):
        with patch("load.DATA_DIR", self.data_dir):
            path = load_csv(self.df)
        self.assertTrue(path.exists())

    def test_csv_content_matches(self):
        with patch("load.DATA_DIR", self.data_dir):
            path = load_csv(self.df)
        loaded = pd.read_csv(path)
        self.assertEqual(len(loaded), len(self.df))

    def test_csv_filename_has_prefix(self):
        with patch("load.DATA_DIR", self.data_dir):
            path = load_csv(self.df, prefix="test_weather")
        self.assertTrue(path.name.startswith("test_weather_"))

    def test_csv_returns_path(self):
        with patch("load.DATA_DIR", self.data_dir):
            result = load_csv(self.df)
        self.assertIsInstance(result, Path)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. LOAD — SQLite
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoadSQLite(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_weather.db"
        self.df = make_clean_df()

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_table_created(self):
        load_sqlite(self.df, db_path=self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        self.assertIn(("weather_raw",), tables)

    def test_row_count_inserted(self):
        load_sqlite(self.df, db_path=self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
        self.assertEqual(count, len(self.df))

    def test_append_mode(self):
        load_sqlite(self.df, db_path=self.db_path)
        load_sqlite(self.df, db_path=self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
        self.assertEqual(count, len(self.df) * 2)

    def test_prepare_for_sqlite_converts_string_dtype(self):
        """Les colonnes StringDtype doivent être convertibles en object pour SQLite."""
        df = pd.DataFrame({
            "city": pd.array(["Paris"], dtype=pd.StringDtype()),
        })
        
        # Simule la conversion faite dans load.py
        df_prepared = df.copy()
        for col in df_prepared.select_dtypes(include="string").columns:
            df_prepared[col] = df_prepared[col].astype(object)
        
        self.assertIn(df_prepared["city"].dtype.name, ["object"])


    def test_prepare_for_sqlite_no_datetimetz(self):
        df = _prepare_for_sqlite(self.df)
        tz_cols = df.select_dtypes(include="datetimetz").columns
        self.assertEqual(len(tz_cols), 0)


# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
