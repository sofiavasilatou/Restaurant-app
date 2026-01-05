# Restaurant App

A small restaurant management suite (GUI + kitchen subscriber + analytics) using SQLite and MQTT.

## Contents
- `apptelefteo.py` — Main desktop GUI and database access layer.
- `kitchen_subscriber.py` — Lightweight Tk-based kitchen UI that subscribes to live orders via MQTT.
- `analytics.py` — Pandas/Matplotlib scripts for plotting receipts, tips and popular dishes.
- `restaurant3telefteo.sqlite` — SQLite database used by the application.

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

## Running

- Start the main app (GUI):

```powershell
python apptelefteo.py
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
The app uses the SQLite file `restaurant3telefteo.sqlite` in the project root. It contains tables for `ORDERS`, `RECEIPT`, `MENU_ITEM`, reservations, clients and staff.

## Notes
- `apptelefteo.py` uses Nominatim (OpenStreetMap) and OSRM for geocoding and routing — these are remote services and have usage policies.
- The GUI and kitchen app use MQTT for live order flow. Ensure your MQTT broker is reachable from both devices.

## License & Contact
This repository contains university project code. Contact: sofizug@gmail.com or 04anasta@gmail.com
