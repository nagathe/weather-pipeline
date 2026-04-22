import requests
from config import API_KEY, BASE_URL

params = {
    "q": "Paris",
    "appid": API_KEY,
    "units": "metric"
}

response = requests.get(BASE_URL, params=params)

print("Status code:", response.status_code)
print("Response JSON:")
print(response.json())
