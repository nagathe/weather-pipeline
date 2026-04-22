import requests
import json

from datetime import datetime
from config import API_KEY, BASE_URL

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