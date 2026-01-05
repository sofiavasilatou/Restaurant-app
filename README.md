# Restaurant App

A small restaurant management suite (GUI + kitchen subscriber + analytics) using SQLite and MQTT.

## Contents
- `app.py` — Main desktop GUI and database access layer.
- `kitchen_subscriber.py` — Lightweight Tk-based kitchen UI that subscribes to live orders via MQTT.
- `analytics.py` — Pandas/Matplotlib scripts for plotting receipts, tips and popular dishes.
- `restaurant.sqlite` — SQLite database used by the application.

## Requirements
- Python 3.8+
- Packages: `paho-mqtt`, `pandas`, `matplotlib`

Install packages manually or in a virtualenv:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate
pip install paho-mqtt pandas matplotlib
```

## Environment variables
- `MQTT_BROKER_HOST` — MQTT broker host (default: `localhost`)
- `MQTT_BROKER_PORT` — MQTT broker port (default: `1883`)
- `RESTAURANT_BRANCH` — branch name used in MQTT topics (default: `mybranch`)
- `MQTT_USER`, `MQTT_PASS` — optional MQTT credentials

Example (PowerShell):

```powershell
$env:MQTT_BROKER_HOST = 'broker.example.com'
$env:RESTAURANT_BRANCH = 'patras-1'
```
These environment variables are optional — the application defaults to `localhost:1883` for the MQTT broker and uses the branch name `mybranch` for topics. Only set them if your MQTT broker is on a different host/port or you want to use a different branch/topic namespace.

## Running

- Start the main app (GUI):

```powershell
python app.py
```

- Run the kitchen view on a kitchen device or VM:

```powershell
python kitchen_subscriber.py
```

- Run analytics (uses the bundled DB):

```powershell
python analytics.py
```

## Database
The app uses the SQLite file `restaurant.sqlite` in the project root. Some of the main tables it contains are for `ORDERS`, `RECEIPT`, `MENU_ITEM`, `RESSERVATIONS`, `CLIENTS`,and `WAITER`  

## Notes
- `app.py` uses Nominatim (OpenStreetMap) and OSRM for geocoding and routing — these are remote services.
- The GUI and kitchen app use MQTT for live order flow. Ensure your MQTT broker is reachable from both devices.

## License & Contact
This repository contains university project code. Contact: sofizug@gmail.com or 04anasta@gmail.com
