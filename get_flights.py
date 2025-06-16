import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("AIRLINE_API")  
# FLIGHT_DATE = "2025-05-16"
URL = "http://api.aviationstack.com/v1/flights"

params = {
    "access_key": API_KEY
}

# === 呼叫 API ===
response = requests.get(URL, params=params)

if response.status_code == 200:
    data = response.json()
    flights = data.get("data", [])

    records = []
    for flight in flights:
        flight_number = (flight.get("flight") or {}).get("number") or ""
        flight_date = flight.get("flight_date") or ""
        if not flight_number or not flight_date:
            continue  # 跳過缺欄位資料

        flight_id = f"{flight_number}_{flight_date}"

        aircraft = flight.get("aircraft") or {}
        aircraft_type = aircraft.get("iata") or ""

        departure = flight.get("departure") or {}
        arrival = flight.get("arrival") or {}
        airline = flight.get("airline") or {}

        record = {
            "flight_id": flight_id,
            "flight_date": flight_date,
            "flight_status": flight.get("flight_status") or "",

            # airline
            "airline_name": airline.get("name") or "",

            # flight
            "flight_number": flight_number,

            # aircraft
            "aircraft_type": aircraft_type,

            # departure
            "dep_airport_name": departure.get("airport") or "",
            "dep_airport_iata": departure.get("iata") or "",
            "dep_delay": departure.get("delay") if departure.get("delay") is not None else "",
            "dep_scheduled": departure.get("scheduled") or "",
            "dep_actual": departure.get("actual") or "",

            # arrival
            "arr_airport_name": arrival.get("airport") or "",
            "arr_airport_iata": arrival.get("iata") or "",
            "arr_delay": arrival.get("delay") if arrival.get("delay") is not None else "",
            "arr_scheduled": arrival.get("scheduled") or "",
            "arr_actual": arrival.get("actual") or "",

            "updated_at": datetime.utcnow()
        }
        records.append(record)

    # 儲存為 CSV
    df = pd.DataFrame(records)
    df.to_csv("raw/flights_raw.csv", index=False)
    try:
        print("Response error message:", response.json())
    except Exception:
        print("Response text:", response.text)