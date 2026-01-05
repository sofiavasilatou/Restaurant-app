import sqlite3
import hashlib
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from datetime import datetime, timedelta
import random
import os
import json
import re
import urllib.parse
import urllib.request
import json
import math
import threading
import time
import queue
import paho.mqtt.client as mqtt

DB_FILE = "restaurant.sqlite"

# Restaurant coordinates (latitude, longitude) 
RESTAURANT_COORDS = (38.2597916,21.7389367)  
CONTACT_EMAIL = "sofizug@gmail.com"  
GEOCODE_TIMEOUT = 10
GEOCODE_RETRIES = 3
GEOCODE_RETRY_DELAY = 1.0
DEFAULT_COUNTRY = "Greece"
DEFAULT_CITY = "Patras"

# Bounding box for Patras 
PATRAS_MIN_LON = 21.726654
PATRAS_MIN_LAT = 38.237610
PATRAS_MAX_LON = 21.82
PATRAS_MAX_LAT = 38.29

OPEN_TIME = 14      # 14:00
CLOSE_TIME = 22     # 22:00
SLOT_DURATION = 2   # hours per slot


def _geocode_address(address: str):
    if not address:
        return None

    variants = [address]
    try:
        low = address.lower()
        if DEFAULT_CITY and DEFAULT_CITY.lower() not in low:
            variants.append(f"{address}, {DEFAULT_CITY}")
            if DEFAULT_COUNTRY and DEFAULT_COUNTRY.lower() not in low:
                variants.append(f"{address}, {DEFAULT_CITY}, {DEFAULT_COUNTRY}")
        if DEFAULT_COUNTRY and DEFAULT_COUNTRY.lower() not in low:
            variants.append(f"{address}, {DEFAULT_COUNTRY}")
    except Exception:
        pass

    last_err = None
    for q in variants:
        for attempt in range(GEOCODE_RETRIES):
            try:
                viewbox = f"{PATRAS_MIN_LON},{PATRAS_MIN_LAT},{PATRAS_MAX_LON},{PATRAS_MAX_LAT}"
                url = (
                    "https://nominatim.openstreetmap.org/search?format=json&limit=1&bounded=1&viewbox="
                    + urllib.parse.quote(viewbox)
                    + "&q="
                    + urllib.parse.quote(q)
                )
                headers = {
                    "User-Agent": f"RestaurantApp/1.0 ({CONTACT_EMAIL})",
                    "From": CONTACT_EMAIL,
                }
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=GEOCODE_TIMEOUT) as resp:
                    data = resp.read().decode("utf-8")
                    arr = json.loads(data)
                    if not arr:
                        last_err = "no results"
                        break
                    lat = float(arr[0]["lat"])
                    lon = float(arr[0]["lon"])
                    if not (PATRAS_MIN_LAT <= lat <= PATRAS_MAX_LAT and PATRAS_MIN_LON <= lon <= PATRAS_MAX_LON):
                        last_err = "result outside Patras bbox"
                        break
                    return (lat, lon)
            except Exception as e:
                last_err = str(e)
                time.sleep(GEOCODE_RETRY_DELAY)
                continue

    return None


def publish_order_event(payload: dict):
    try:
        broker = os.environ.get("MQTT_BROKER_HOST", "localhost")
        port = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
        branch = os.environ.get("RESTAURANT_BRANCH", "mybranch")
        topic = f"restaurant/{branch}/orders/new"
        client = mqtt.Client()
        try:
            client.connect(broker, port, 60)
            client.loop_start()
            try:
                info = client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=1)
                info.wait_for_publish(timeout=5)
            except Exception as e:    
                print("publish_order_event: publish failed:", e)
                
                client.disconnect()
                client.loop_stop()
            
        except Exception as e:
            print(f"publish_order_event: failed to publish to {broker}:{port} topic {topic}:", e)
            
            client.disconnect()
    except Exception:
        print("publish_order_event: unexpected error")
        return


_ready_mqtt_client = None
_ready_msg_queue = queue.Queue()

def start_ready_listener():
    """Start a background MQTT client that listens for order ready notifications
    and enqueues them in `_ready_msg_queue` for the UI to poll.
    """
    global _ready_mqtt_client
    # If a listener is already running, return it to avoid duplicate clients
    if _ready_mqtt_client is not None:
        return _ready_mqtt_client
   
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            branch = os.environ.get("RESTAURANT_BRANCH", "mybranch")
            topic = f"restaurant/{branch}/orders/ready"
            client.subscribe(topic, qos=1)


    def on_message(client, userdata, msg):
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        _ready_msg_queue.put(data)
       
    try:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        broker = os.environ.get("MQTT_BROKER_HOST", "localhost")
        port = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
        try:
            client.connect(broker, port, 60)
        except Exception:
            return None
        client.loop_start()
        _ready_mqtt_client = client
        return client
    except Exception:
        return None


def stop_ready_listener():
    global _ready_mqtt_client
    try:
        if _ready_mqtt_client:
            _ready_mqtt_client.loop_stop()
            _ready_mqtt_client.disconnect()
    finally:
        _ready_mqtt_client = None


def _geocode_debug(address: str):
    if not address:
        return None, "empty address"

    variants = [address]
    try:
        low = address.lower()
        if DEFAULT_CITY and DEFAULT_CITY.lower() not in low:
            variants.append(f"{address}, {DEFAULT_CITY}")
            if DEFAULT_COUNTRY and DEFAULT_COUNTRY.lower() not in low:
                variants.append(f"{address}, {DEFAULT_CITY}, {DEFAULT_COUNTRY}")
        if DEFAULT_COUNTRY and DEFAULT_COUNTRY.lower() not in low:
            variants.append(f"{address}, {DEFAULT_COUNTRY}")
    except Exception:
        pass

    last_err = None
    for q in variants:
        for attempt in range(GEOCODE_RETRIES):
            try:
                # Restrict search to Patras bounding box to avoid picking addresses elsewhere.
                viewbox = f"{PATRAS_MIN_LON},{PATRAS_MIN_LAT},{PATRAS_MAX_LON},{PATRAS_MAX_LAT}"
                url = (
                    "https://nominatim.openstreetmap.org/search?format=json&limit=1&bounded=1&viewbox="
                    + urllib.parse.quote(viewbox)
                    + "&q="
                    + urllib.parse.quote(q)
                )
                headers = {
                    "User-Agent": f"RestaurantApp/1.0 ({CONTACT_EMAIL})",
                    "From": CONTACT_EMAIL,
                }
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=GEOCODE_TIMEOUT) as resp:
                    data = resp.read().decode("utf-8")
                    arr = json.loads(data)
                    if not arr:
                        last_err = "no results"
                        break
                    lat = float(arr[0]["lat"])
                    lon = float(arr[0]["lon"])
                    if not (PATRAS_MIN_LAT <= lat <= PATRAS_MAX_LAT and PATRAS_MIN_LON <= lon <= PATRAS_MAX_LON):
                        last_err = "result outside Patras bbox"
                        break
                    return (lat, lon), None
            except Exception as e:
                last_err = str(e)
                time.sleep(GEOCODE_RETRY_DELAY)
                continue

    return None, last_err or "unknown error"

def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


def _route_duration_osrm(from_lat, from_lon, to_lat, to_lon, profile="driving"):
    try:
        coords = f"{from_lon},{from_lat};{to_lon},{to_lat}"
        url = f"https://router.project-osrm.org/route/v1/{profile}/{coords}?overview=false&annotations=duration,distance"
        headers = {"User-Agent": f"RestaurantApp/1.0 ({CONTACT_EMAIL})", "From": CONTACT_EMAIL}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=GEOCODE_TIMEOUT) as resp:
            data = resp.read().decode("utf-8")
            obj = json.loads(data)
            if obj.get("code") != "Ok":
                return None, f"OSRM error: {obj.get('message', obj.get('code'))}"
            routes = obj.get("routes")
            if not routes:
                return None, "OSRM no routes"
            duration_s = routes[0].get("duration")
            distance_m = routes[0].get("distance")
            if duration_s is None:
                return None, "OSRM missing duration"
            minutes = max(1, int(round(duration_s / 60.0)))
            dbg = f"OSRM: {distance_m/1000:.2f} km, {minutes} min"
            return minutes, dbg
    except Exception as e:
        return None, str(e)


def estimate_travel_minutes(address: str, speed_kmh: float = 35.0, fallback_minutes: int = 30, debug: bool = False):
    """Estimate travel minutes from restaurant to address.

    Behavior:
    - Geocode via Nominatim
    - Try OSRM route duration (preferred)
    - Fall back to straight-line haversine + `speed_kmh`
    - If geocoding fails, return `fallback_minutes`.

    If `debug` is False (default) returns an `int` minutes (backwards compatible).
    If `debug` is True returns a tuple `(minutes, source, debug_str)` where
    `source` is one of: 'osrm', 'haversine', 'geocode-failed'.
    """
    coords = _geocode_address(address)
    if not coords:
        if debug:
            return (fallback_minutes, "geocode-failed", "no geocode result")
        return fallback_minutes

    lat, lon = coords
    rlat, rlon = RESTAURANT_COORDS

    # Try OSRM routing first
    minutes_osrm, dbg = _route_duration_osrm(rlat, rlon, lat, lon)
    if minutes_osrm is not None:
        minutes = max(5, minutes_osrm)
        if debug:
            return (minutes, "osrm", dbg)
        return minutes

    # Fallback: straight-line estimate
    dist_km = _haversine_km(rlat, rlon, lat, lon)
    if dist_km < 0.05:
        if debug:
            return (5, "haversine", "very short distance")
        return 5
    minutes = int(round((dist_km / max(0.1, speed_kmh)) * 60.0))
    minutes = max(5, minutes)
    if debug:
        return (minutes, "haversine", f"dist {dist_km:.2f} km, speed {speed_kmh} km/h")
    return minutes


def convert_ddmmyyyy_to_iso(date_text):
        try:
            d, m, y = date_text.split("-")
            return f"{y}-{m}-{d}"
        except:
            return None
        
class Database:
    def __init__(self, db_file=DB_FILE):
        self.db_file = db_file
        self._ensure_schema()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _ensure_schema(self):
        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(WAITER)")
        cols = [c[1] for c in cur.fetchall()]
        if "password_hash" not in cols:
            try:
                cur.execute("ALTER TABLE WAITER ADD COLUMN password_hash TEXT")
            except Exception:
                pass

        conn.commit()
        conn.close()

    def setpasswords(self, waiter_id, new_password):
        hashed = hashlib.sha256(new_password.encode()).hexdigest()
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE WAITER SET password_hash = ? WHERE waiter_id = ?",
            (hashed, waiter_id)
        )
        conn.commit()
        conn.close()

    def generate_time_slots(self):
        slots = []
        for hour in range(OPEN_TIME, CLOSE_TIME - SLOT_DURATION + 1):
            start = f"{hour:02d}:00"
            end = f"{hour + SLOT_DURATION:02d}:00"
            slots.append((start, end))
        return slots

    def table_is_available_for_slot(self, table_no, reserv_date, start_time, end_time):
        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT 1
            FROM RESERVATION r
            JOIN RESERVATION_CORRESPONDS_TO_TABLE rt
                ON r.reserv_no = rt.reserv_no
            WHERE rt.table_no = ?
            AND r.date = ?
            AND NOT (
                r.end_time <= ?
                OR r.start_time >= ?
            )
        """, (table_no, reserv_date, start_time, end_time))

        row = cur.fetchone()
        conn.close()
        return row is None

    def available_slots_for_table(self, table_no, reserv_date):
        slots = self.generate_time_slots()
        available = []

        for start, end in slots:
            if self.table_is_available_for_slot(table_no, reserv_date, start, end):
                available.append((start, end))

        return available

    def max_available_end_time(self, table_no, reserv_date, requested_start):
        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT MIN(r.start_time)
            FROM RESERVATION r
            JOIN RESERVATION_CORRESPONDS_TO_TABLE rt
            ON r.reserv_no = rt.reserv_no
            WHERE rt.table_no = ?
            AND r.date = ?
            AND r.start_time > ?
        """, (table_no, reserv_date, requested_start))

        row = cur.fetchone()
        conn.close()

        if row and row[0]:
            return row[0]   
        return None        

        def reserved_tables_for_timeslot(self, reserv_date, start_time, end_time):
                """
                Return a set of table_no values that have a reservation overlapping
                the given [start_time, end_time) on the given date (YYYY-MM-DD).
                """
                conn = self._get_conn()
                cur = conn.cursor()
                cur.execute("""
                        SELECT rt.table_no
                        FROM RESERVATION r
                        JOIN RESERVATION_CORRESPONDS_TO_TABLE rt
                            ON r.reserv_no = rt.reserv_no
                        WHERE r.date = ?
                            AND NOT (r.end_time <= ? OR r.start_time >= ?)
                """, (reserv_date, start_time, end_time))
                rows = cur.fetchall()
                conn.close()
                return {r[0] for r in rows}


    def slot_availability_status(self, table_no, reserv_date, start_time):
        """
        Returns:
                
        "full"    → διαθέσιμο για >= SLOT_DURATION
        "partial" → διαθέσιμο για >= 1 ώρα αλλά < SLOT_DURATION
        "blocked" → < 1 ώρα διαθέσιμη"""
        max_end = self.max_available_end_time(table_no, reserv_date, start_time)

        if not max_end:
            return "full"

        start_dt = datetime.strptime(start_time, "%H:%M")
        end_dt = datetime.strptime(max_end, "%H:%M")
        minutes = (end_dt - start_dt).total_seconds() / 60

        if minutes >= SLOT_DURATION * 60:
            return "full"
        elif minutes >= 60:
            return "partial"
        else:
            return "blocked"
        
    def verify_waiter(self, username, password):
        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT waiter_id, first_name, password_hash
            FROM WAITER
            WHERE LOWER(first_name) = LOWER(?)
            """,
            (username,),
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return False, None

        waiter_id, first_name, password_hash = row
        calc = hashlib.sha256(password.encode()).hexdigest()
        if calc == password_hash:
            return True, {"waiter_id": waiter_id, "first_name": first_name}
        return False, None

    #reservation methods
    def create_client(self, first_name, last_name, email, phone):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO CLIENT (first_name, last_name, email, phone)
            VALUES (?, ?, ?, ?)
            """,
            (first_name, last_name, email, phone or None),
        )
        client_id = cur.lastrowid
        conn.commit()
        conn.close()
        return client_id

    def list_tables(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_no, capacity, view, location 
            FROM TABLE_RESTAURANT
            ORDER BY table_no
            """
        )
        rows = cur.fetchall()
        conn.close()
        return rows

    def list_all_orders(self):
        """Return a combined list of orders (in-place and online).
        Each row: (order_id, datetime, kind, table_no, client_id, address, estimated_time, comments, paid)
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            
            cur.execute(
                """
                SELECT o.order_id, o.datetime,
                       CASE WHEN ip.order_id IS NOT NULL THEN 'in_place' WHEN onl.order_id IS NOT NULL THEN 'online' ELSE 'unknown' END AS kind,
                       ip.table_no,
                       onl.client_id,
                       onl.address,
                       onl.estimated_time,
                       onl.comments,
                           COALESCE(MAX(r.paid_off), 0) AS paid
                FROM ORDERS as o
                LEFT JOIN IN_PLACE_ORDER as ip ON ip.order_id = o.order_id
                LEFT JOIN ONLINE_ORDER as onl ON onl.order_id = o.order_id
                LEFT JOIN RECEIPT as r ON r.order_id = o.order_id
                GROUP BY o.order_id
                ORDER BY o.datetime DESC
                """
            )
            rows = cur.fetchall()
        except Exception:
            # Fallback: try older query without receipts (returns paid as 0)
            try:
                cur.execute(
                    """
                    SELECT o.order_id, o.datetime,
                           CASE WHEN ip.order_id IS NOT NULL THEN 'in_place' WHEN onl.order_id IS NOT NULL THEN 'online' ELSE 'unknown' END AS kind,
                           ip.table_no,
                           onl.client_id,
                           onl.address,
                           onl.estimated_time,
                           onl.comments,
                           0 AS paid
                    FROM ORDERS as o
                    LEFT JOIN IN_PLACE_ORDER as ip ON ip.order_id = o.order_id
                    LEFT JOIN ONLINE_ORDER as onl ON onl.order_id = o.order_id
                    ORDER BY o.datetime DESC
                    """
                )
                rows = cur.fetchall()
            except Exception:
                rows = []
        finally:
            conn.close()
        return rows

    def _next_reservation_no(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(CAST(reserv_no AS INTEGER)) FROM RESERVATION")
        row = cur.fetchone()
        conn.close()

        max_no = row[0]
        if max_no is None:
            return "100"
        return str(int(max_no) + 1)

    def create_reservation(
        self,
        waiter_id,
        client_data,
        reserv_date, 
        start_time,
        end_time,
        people_number,
        table_no,
    ):
        try:
            parsed = datetime.strptime(reserv_date, "%d-%m-%Y")
            reserv_date_sql = parsed.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError("Date must be in format DD-MM-YYYY")

        client_id = self.create_client(
            client_data["first_name"],
            client_data["last_name"],
            client_data["email"],
            client_data["phone"],
        )

        reserv_no = self._next_reservation_no()

        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO RESERVATION
                (reserv_no, start_time, end_time, date, people_number, client_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (reserv_no, start_time, end_time, reserv_date_sql, people_number, client_id),
        )

        # Link reservation to table- single or iterable
        try:
            if isinstance(table_no, (list, tuple, set)):
                for t in table_no:
                    cur.execute(
                        "INSERT INTO RESERVATION_CORRESPONDS_TO_TABLE (table_no, reserv_no) VALUES (?, ?)",
                        (t, reserv_no),
                    )
                    
            else:
                cur.execute(
                    "INSERT INTO RESERVATION_CORRESPONDS_TO_TABLE (table_no, reserv_no) VALUES (?, ?)",
                    (table_no, reserv_no),
                )

        except Exception:
            cur.execute(
                "INSERT INTO RESERVATION_CORRESPONDS_TO_TABLE (table_no, reserv_no) VALUES (?, ?)",
                (table_no, reserv_no),
            )

        conn.commit()
        conn.close()
        return reserv_no

    def list_reservations(self):
        """Return list of reservations with joined client and table info.
        Each row: (reserv_no, date, start_time, end_time, people_number, client_first, client_last, phone, tables)
        where tables is a comma-separated string of table numbers.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT r.reserv_no, r.date, r.start_time, r.end_time, r.people_number,
                       c.first_name, c.last_name, c.phone,
                       GROUP_CONCAT(rt.table_no) as tables
                FROM RESERVATION r
                LEFT JOIN CLIENT c ON c.client_id = r.client_id
                LEFT JOIN RESERVATION_CORRESPONDS_TO_TABLE rt ON rt.reserv_no = r.reserv_no
                GROUP BY r.reserv_no
                ORDER BY r.date, r.start_time
                """
            )
            rows = cur.fetchall()
        except Exception:
            rows = []
        finally:
            conn.close()
        return rows

    def delete_reservation(self, reserv_no):
        """Delete reservation and its table links. Return True on success."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            # find linked tables
            try:
                cur.execute("SELECT table_no FROM RESERVATION_CORRESPONDS_TO_TABLE WHERE reserv_no = ?", (reserv_no,))
                tabs = [r[0] for r in cur.fetchall()]
            except Exception:
                tabs = []

            cur.execute("DELETE FROM RESERVATION_CORRESPONDS_TO_TABLE WHERE reserv_no = ?", (reserv_no,))
            cur.execute("DELETE FROM RESERVATION WHERE reserv_no = ?", (reserv_no,))         

            conn.commit()
            return True
        except Exception:
            conn.rollback()
            return False
        finally:
            conn.close()

# menu item methods
    def list_menu_items(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT item_id, description, net_price, availability, allergens, tax_name
            FROM MENU_ITEM
            ORDER BY item_id
            """
        )
        rows = cur.fetchall()
        conn.close()
        return rows

    def _next_order_id(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(order_id) FROM ORDERS")
        row = cur.fetchone()
        conn.close()
        max_id = row[0]
        if max_id is None:
            return 500
        return int(max_id) + 1

    def create_in_place_order(self, waiter_id, table_no, items):
        if not items:
            raise ValueError("Order must contain at least one item.")

        order_id = self._next_order_id()
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M")

        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO ORDERS (order_id, datetime) VALUES (?, ?)",
            (order_id, now_text),
        )

        cur.execute(
            """
            INSERT INTO IN_PLACE_ORDER (order_id, waiter_id, table_no)
            VALUES (?, ?, ?)
            """,
            (order_id, waiter_id, table_no),
        )

        for item_id, qty in items:
            cur.execute(
                """
                INSERT INTO ORDER_CONSISTS_OF_MENU_ITEM (order_id, item_id, quantity)
                VALUES (?, ?, ?)
                """,
                (order_id, item_id, qty),
            )

        # Create a single receipt_no for this order and insert per-item receipt rows
        try:
            cur.execute("SELECT MAX(receipt_no) FROM RECEIPT")
            r = cur.fetchone()
            next_receipt_no = 9001 if not r or r[0] is None else int(r[0]) + 1
        except Exception:
            # RECEIPT table might not exist; skip receipt creation 
            next_receipt_no = None

        now_time = datetime.now().strftime("%H:%M")
        today = datetime.now().strftime("%Y-%m-%d")

        if next_receipt_no is not None:
            for item_id, qty in items:
                try:
                    cur.execute("SELECT net_price FROM MENU_ITEM WHERE item_id = ?", (item_id,))
                    row = cur.fetchone()
                    price = float(row[0]) if row and row[0] is not None else 0.0
                except Exception:
                    price = 0.0
                subtotal = qty * price
                
                cur.execute(
                        "INSERT INTO RECEIPT (item_id, qty, tips, total_amount, receipt_no, order_id, paid_off, time, date, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (item_id, qty, 0.0, subtotal, next_receipt_no, order_id, 0, now_time, today, None),
                    )
                

        conn.commit()
        
        items_payload = []
        try:
            for i, q in items:
                desc = None
                try:
                    cur.execute("SELECT description FROM MENU_ITEM WHERE item_id = ?", (i,))
                    r = cur.fetchone()
                    if r and r[0] is not None:
                        desc = r[0]
                except Exception:
                    desc = None
                items_payload.append({"item_id": int(i), "qty": int(q), "description": desc})
        except Exception:
            items_payload = [{"item_id": int(i), "qty": int(q)} for i, q in items]

        payload = {
            "order_id": order_id,
            "kind": "in_place",
            "waiter_id": waiter_id,
            "table_no": table_no,
            "items": items_payload,
            "datetime": now_text,
        }
        
        publish_order_event(payload)
        
        conn.close()
        return order_id


    def table_is_available(self, table_no, reserv_date, reserv_time):
        conn = self._get_conn()
        cur = conn.cursor()

        cur.execute("""
        SELECT r.reserv_no
        FROM RESERVATION r
        JOIN RESERVATION_CORRESPONDS_TO_TABLE rt
        ON r.reserv_no = rt.reserv_no
        WHERE rt.table_no = ?
        AND r.date = ?
        AND r.start_time = ?
        """, (table_no, reserv_date, reserv_time))

        row = cur.fetchone()
        conn.close()

        return row is None  # True = table free

    def mark_order_paid(self, order_id, tip_amount: float = None, in_place_status=0, payment_method=None):
        """Mark order as paid: set ORDERS.paid, IN_PLACE_ORDER.status and RECEIPT.paid_off.
        Optionally store a tip amount (float) in RECEIPT.tips for the given order.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            
            # mark receipt rows for this order as paid
            try:
                cur.execute("UPDATE RECEIPT SET paid_off = 1 WHERE order_id = ?", (order_id,))
            except Exception:
                pass

            # If tip provided we store it on receipt rows for this order
            if tip_amount is not None:
                try:
                    cur.execute("UPDATE RECEIPT SET tips = ? WHERE order_id = ?", (float(tip_amount), order_id))
                except Exception:
                    pass

            # If payment_method provided ensure column exists then store it on receipt rows
            if payment_method is not None:
                
                cur.execute("UPDATE RECEIPT SET payment_method = ? WHERE order_id = ?", (payment_method, order_id))

            cur.execute("UPDATE RECEIPT SET paid_off = ? WHERE order_id = ?", (in_place_status, order_id))
            

            conn.commit()
            return True
        except Exception as e:
            try:
                print(f"mark_order_paid: error for order {order_id}:", repr(e))
            except Exception:
                pass
            conn.rollback()
            return False
        finally:
            conn.close()


class LoginApp(tk.Tk):
    def __init__(self, db: Database):
        super().__init__()
        self.db = db

        try:
            style = ttk.Style(self)
            
            try:
                style.theme_use("clam")
            except Exception:
                for th in ("alt", "default"):
                    try:
                        style.theme_use(th)
                        break
                    except Exception:
                        pass

            # Colors
            bg = "#f6f9fb"      
            fg = "#0f172a"      
            primary = "#eaf4ff" 
            accent = "#10b981"  

            # Root background
            try:
                self.configure(bg=bg)
            except Exception:
                pass

            
            try:
                style.configure("TFrame", background=bg)
                style.configure("TLabel", background=bg, foreground=fg, font=("Segoe UI", 11))
                style.configure("TButton", foreground=fg, padding=6, font=("Segoe UI", 11), background=primary)
                style.configure("TCombobox", fieldbackground="white", background="white")

                style.configure("Accent.TButton", background=primary, foreground="#000000", padding=8, font=("Segoe UI", 11))
                style.map("Accent.TButton",
                          background=[("active", "#d4ecff"), ("pressed", "#c0e4ff")],
                          foreground=[("disabled", "#888888")])
            except Exception:
                pass
        except Exception:
            pass

        # start window at normal size but allow resizing 
        self.bind("<Escape>", lambda e: self.state("normal"))

        self.minsize(1024, 768)

        self.title("Restaurant Login")

        
        self._build_login_ui()

    def _build_login_ui(self):
        frame = ttk.Frame(self, padding=30)
        frame.pack(expand=True, fill="both")

        ttk.Label(
            frame,
            text="Restaurant System",
            font=("Arial", 20, "bold")
        ).pack(pady=(0, 20))

        ttk.Label(frame, text="Waiter Username (first name):").pack(anchor="w")
        self.username_entry = ttk.Entry(frame, width=40)
        self.username_entry.pack(fill="x", pady=5)
        # pressing Enter while focused on username should login
        self.username_entry.bind("<Return>", lambda e: self.attempt_login())

        ttk.Label(frame, text="Password:").pack(anchor="w", pady=(10, 0))
        self.password_entry = ttk.Entry(frame, show="*", width=40)
        self.password_entry.pack(fill="x", pady=5)
        # pressing Enter while focused on password should login
        self.password_entry.bind("<Return>", lambda e: self.attempt_login())

        ttk.Button(frame, text="Waiter Login", command=self.attempt_login, style="Accent.TButton").pack(pady=15)

        ttk.Label(frame, text="OR").pack()
        ttk.Button(
            frame,
            text="Customer",
            command=self.open_customer_mode,
            width=34,
            style="Accent.TButton",
        ).pack(padx=0, pady=36, ipady=8)

    def attempt_login(self):
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()

        ok, waiter = self.db.verify_waiter(username, password)

        if not ok:
            messagebox.showerror("Login failed", "Invalid waiter name or password.")
            return

     # LOGIN SUCCESS
        self.withdraw()
        win = tk.Toplevel()
        WaiterUI(win, self.db, waiter)

    def open_customer_mode(self):
        self.destroy()
        root = tk.Tk()
        CustomerUI(root, self.db)
        root.mainloop()

class WaiterUI(ttk.Frame):
    def __init__(self, master, db: Database, waiter: dict):
        super().__init__(master, padding=10)
        self.master = master
        self.db = db
        self.waiter = waiter

        master.title(f"Waiter Panel – {waiter['first_name']}")
        master.state("zoomed")
        master.minsize(1024, 768)
        master.resizable(True, True)

        ttk.Label(
            self,
            text=f"Welcome, {waiter['first_name']}",
            font=("Arial", 14, "bold"),
        ).pack(pady=10)

        ttk.Button(
            self,
            text="Make Reservation",
            width=25,
            command=self.open_reservation_window,
            style="Accent.TButton",
        ).pack(pady=10)

        ttk.Button(
            self,
            text="Create Table Order",
            width=25,
            command=self.open_order_window,
            style="Accent.TButton",
        ).pack(pady=10)

        ttk.Button(
            self,
            text="All Orders",
            width=25,
            command=self.open_orders_window,
            style="Accent.TButton",
        ).pack(pady=10)

        ttk.Button(
            self,
            text="Reservations",
            width=25,
            command=self.open_reservations_manager,
            style="Accent.TButton",
        ).pack(pady=10)

        ttk.Button(
            self,
            text="Logout",
            width=25,
            command=self.logout,
            style="Accent.TButton",
        ).pack(pady=20)

        self.pack(expand=True, fill="both")
        # start listener for 'order ready' notifications
        try:
            start_ready_listener()
            # poll the ready message queue periodically
            self.after(1000, self._poll_ready)
        except Exception:
            pass

    def _poll_ready(self):
        # check global ready queue and notify waiter
        
        from_queue = globals().get('_ready_msg_queue')
        if from_queue:
            # drain the queue until empty; handle queue.Empty
            try:
                while True:
                    data = from_queue.get_nowait()
                    oid = data.get('order_id') if isinstance(data, dict) else None
                    table = data.get('table_no') if isinstance(data, dict) else None
                    if oid is None:
                        continue
                    if table is not None:
                        msg = f"Order #{oid} (Table {table}) is ready."
                    else:
                        msg = f"Order #{oid} is ready."
                    try:
                        messagebox.showinfo("Order Ready", msg)
                    except Exception:
                        print(msg)
            except queue.Empty:
                pass
                        
        self.after(1000, self._poll_ready)
        

    def open_reservation_window(self):
        
        # hide the waiter main window and open reservation window with a return callback
        self.master.withdraw()
        
        ReservationWindow(self.master, self.db, self.waiter, return_callback=self._restore)

    def open_reservations_manager(self):
        
        self.master.withdraw()
        
        ReservationsManager(self.master, self.db, return_callback=self._restore)

    def _restore(self):
        
            self.master.deiconify()
            
            self.master.state("zoomed")

    def open_order_window(self):
        
        self.master.withdraw()
        OrderWindow(self.master, self.db, self.waiter, return_callback=self._restore)

    def open_orders_window(self):
        
        self.master.withdraw()
        
        OrdersWindow(self.master, self.db, return_callback=self._restore)

    def logout(self):
        try:
            # Restore the main login/root window (if it was hidden) before closing waiter UI
            root = getattr(tk, "_default_root", None)
            if root:
                
                root.deiconify()
                # clear any previous login values for security/privacy
                
                if hasattr(root, 'username_entry'):
                    root.username_entry.delete(0, 'end')
                    
                if hasattr(root, 'password_entry'):    
                    root.password_entry.delete(0, 'end')
                    
                
                root.username_entry.focus_set()
                
                
                root.state("normal")
                
            
            self.master.destroy()

        except Exception:
            self.master.destroy()
           



class ReservationWindow(tk.Toplevel):
    def __init__(self, master, db: Database, waiter: dict = None, client_data: dict = None, return_callback=None):
        super().__init__(master)
        self.db = db
        self.waiter = waiter
        self.client_data = client_data
        self.return_callback = return_callback

        self.title("Create Reservation")
        
        self.withdraw()
        self.update_idletasks()
        self.state("zoomed")
        self.deiconify()
  
        self.geometry("1024x768")
 
        self.bind("<Escape>", lambda e: self.state("normal"))
        self.minsize(1024, 768)

        self.resizable(True, True)

        ttk.Label(self, text="Create Reservation", font=("Arial", 16, "bold")).pack(
            pady=10
        )

        # layout: form (left), visual table map (right)
        container = ttk.Frame(self, padding=10)
        container.pack(fill="both", expand=True, padx=10, pady=10)

        container.grid_columnconfigure(0, weight=3, minsize=360)
        container.grid_columnconfigure(1, weight=5, minsize=640)
        container.grid_rowconfigure(0, weight=1)

        frame = ttk.Frame(container, padding=(8, 8))
        frame.grid(row=0, column=0, sticky="nsew", padx=(0,6), pady=0)
        frame.grid_columnconfigure(1, weight=1)
        

        viz_frame = ttk.Frame(container)
        viz_frame.grid(row=0, column=1, sticky="nsew", padx=(6,0), pady=0)

        # Location filter control (inside/outside) shown above the map
        self.location_filter_var = tk.StringVar(value="Μέσα")
        loc_ctrl = ttk.Frame(viz_frame)
        loc_ctrl.pack(fill="x", padx=8, pady=(0,6))
        ttk.Label(loc_ctrl, text="Reservation Area:").pack(side="left")
        loc_cb = ttk.Combobox(loc_ctrl, textvariable=self.location_filter_var, values=("Μέσα", "Έξω"), width=12, state="readonly")
        loc_cb.pack(side="left", padx=(8,0))
        loc_cb.bind("<<ComboboxSelected>>", lambda e: self._draw_table_map())

        # CLIENT FIELDS 
        ttk.Label(frame, text="Client First Name:").grid(row=0, column=0, sticky="e", pady=8)
        self.client_first = ttk.Entry(frame, width=48)
        self.client_first.grid(row=0, column=1, sticky="ew", pady=8)

        ttk.Label(frame, text="Client Last Name:").grid(row=1, column=0, sticky="e", pady=8)
        self.client_last = ttk.Entry(frame, width=48)
        self.client_last.grid(row=1, column=1, sticky="ew", pady=8)

        ttk.Label(frame, text="Client Email:").grid(row=2, column=0, sticky="e", pady=8)
        self.client_email = ttk.Entry(frame, width=48)
        self.client_email.grid(row=2, column=1, sticky="ew", pady=8)

        ttk.Label(frame, text="Client Phone:").grid(row=3, column=0, sticky="e", pady=8)
        self.client_phone = ttk.Entry(frame, width=48)
        self.client_phone.grid(row=3, column=1, sticky="ew", pady=8)

        # RESERVATION FIELDS 

        ttk.Label(frame, text="Date (DD-MM-YYYY):").grid(row=4, column=0, sticky="e", pady=8)
        self.date_entry = ttk.Entry(frame, width=30)
        self.date_entry.grid(row=4, column=1, sticky="ew", pady=8)

        # Time slots area
        ttk.Label(frame, text="Time (slots):").grid(row=5, column=0, sticky="ne", pady=8)
        self.time_frame = ttk.Frame(frame)
        self.time_frame.grid(row=5, column=1, sticky="ew", pady=8)
        self.selected_slot = None

        # Hint label shown under time slots 
        self.slot_hint_label = ttk.Label(frame, text="", foreground="#555")
        self.slot_hint_label.grid(row=6, column=1, sticky="w", pady=(6,0))

        ttk.Label(frame, text="People:").grid(row=8, column=0, sticky="e", pady=12)
        self.people_entry = ttk.Entry(frame, width=10)
        self.people_entry.grid(row=8, column=1, sticky="w", pady=12)
        # Inline capacity hint shown under the People field (red when insufficient)
        self.people_hint_label = ttk.Label(frame, text="", foreground="#b91c1c")
        self.people_hint_label.grid(row=9, column=1, sticky="w", pady=(0,8))
        # Update capacity hint as the user types the people count
        
        self.people_entry.bind("<KeyRelease>", lambda e: self._check_capacity())     
        self.people_entry.insert(0, "2")

        ttk.Label(frame, text="Table:").grid(row=10, column=0, sticky="e", pady=8)
        self.table_choice_label = ttk.Label(frame, text="Choose a table from the table map", foreground="#555")
        self.table_choice_label.grid(row=10, column=1, sticky="w", pady=8)

        # Visual table map
        self.selected_tables = set()
        self.table_canvas = tk.Canvas(viz_frame, bg="#f6f9fb", highlightthickness=0)
        self.table_canvas.pack(fill="both", expand=True, padx=10, pady=6)
        # Redraw map when the canvas is resized so shapes and fonts scale
        self.table_canvas.bind("<Configure>", lambda e: self._draw_table_map())
        
        self.table_canvas_items = {}
        legend = ttk.Label(viz_frame, text="Click boxes to select tables\n(hold multiple for adjacent)", justify="center")
        legend.config(font=(None, 9))
        legend.pack(padx=8, pady=(0,8))

        self._load_tables()

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=11, column=0, columnspan=2, sticky="w", pady=(14,0))

        if self.waiter is None:
            tk.Button(btn_frame, text="Save Reservation", command=self.save_reservation, width=16,
                      bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
                      activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(side="left", padx=8, pady=6)
        else:
            ttk.Button(btn_frame, text="Save Reservation", command=self.save_reservation, width=16, style="Accent.TButton").pack(side="left", padx=8, pady=6)

        if self.return_callback:
            if self.waiter is None:
                tk.Button(btn_frame, text="Back", command=self._on_back, width=12,
                          bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
                          activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(side="left", padx=8, pady=6)
            else:
                ttk.Button(btn_frame, text="Back", command=self._on_back, width=12, style="Accent.TButton").pack(side="left", padx=8, pady=6)

        self.date_entry.bind("<KeyRelease>", lambda e: self._load_time_slots())
        self.after(200, self._load_time_slots)

    # TIME SLOTS
    def _load_time_slots(self):
        date_txt = self.date_entry.get().strip()
        # Validate date as the user types and show an inline blue hint if invalid
        if date_txt:
            try:
                parsed = datetime.strptime(date_txt, "%d-%m-%Y")
                today = datetime.now().date()
                if parsed.date() <= today:
                        # Blue hint: require reservations from tomorrow onwards
                    self.slot_hint_label.config(text="Reservations must be from tomorrow onwards", foreground="#2563eb")
                    
                    # If date is not valid for reservations stop
                    return
                else:
                    # valid future date 
                    self.slot_hint_label.config(text="", foreground="#555")
                    
            except Exception:
                    # correct date format while typing
                    self.slot_hint_label.config(text="Enter date as DD-MM-YYYY", foreground="#2563eb")
                    return

        # Allow rendering time slots when a valid future date is entered even if no table has been selected yet. If a table is selected, prefer
        # its availability, otherwise show general slots
        first = None
        table_label = None
        if getattr(self, "selected_tables", None) and len(self.selected_tables) > 0:
            first = sorted(self.selected_tables)[0]
            table_label = getattr(self, "table_label_by_no", {}).get(first)
        # If we have neither a date nor a table label at this point, stop
        if not date_txt:
            return

        date_iso = parsed.strftime("%Y-%m-%d")

        # If a table was selected, query availability for that table and date.
        table_no = None
        if table_label:
            table_no = self.table_map.get(table_label)

        if table_no is None:
            available = None
        else:
            available = []

            for start, end in self.db.generate_time_slots():
                # πλήρως διαθέσιμο (2h)
                if self.db.table_is_available_for_slot(table_no, date_iso, start, end):
                    available.append((start, end))
                    continue

                # έλεγχος για partial (1h)
                status = self.db.slot_availability_status(
                    table_no=table_no,
                    reserv_date=date_iso,
                    start_time=start
                )

                if status == "partial":
                    available.append((start, end))
        self._render_time_slots(available)
        self.slot_hint_label.config(text="")        
        self._draw_table_map(selected_date_iso=date_iso)

    def _render_time_slots(self, available_slots):
        
        date_iso = None
        try:
            date_txt = self.date_entry.get().strip()
            parsed = datetime.strptime(date_txt, "%d-%m-%Y")
            if parsed.date() > datetime.now().date():
                date_iso = parsed.strftime("%Y-%m-%d")
        except Exception:
            pass

        # resolve table_no
        table_no = None
        if getattr(self, "selected_tables", None) and len(self.selected_tables) > 0:
            table_no = sorted(self.selected_tables)[0]

        
        for widget in self.time_frame.winfo_children():
            widget.destroy()

        self.selected_slot = None

        all_slots = self.db.generate_time_slots()

        if not available_slots:
            available_set = set(all_slots)
        else:
            available_set = set(available_slots)

        col = row = 0
        for start, end in all_slots:
            is_available = (start, end) in available_set

            # default values
            slot_type = "full"
            label = start
            bg = "#2563eb"
            fg = "white"
            state = "normal"

            # check partial availability (1h) 
            if is_available and table_no and date_iso:
                try:
                    status = self.db.slot_availability_status(
                        table_no=table_no,
                        reserv_date=date_iso,
                        start_time=start
                    )
                    if status == "partial":
                        slot_type = "partial"
                except Exception:
                    pass

            
            if not is_available:
                bg = "#e5e7eb"
                fg = "#9ca3af"
                state = "disabled"
                label = start

            elif slot_type == "partial":
                bg = "#f59e0b"      
                fg = "white"
                state = "normal"
                label = f"{start}\n1h"

            else:
                bg = "#2563eb"      
                fg = "white"
                state = "normal"
                label = start

            btn = tk.Button(
                self.time_frame,
                text=label,
                width=8,
                height=2,
                relief="solid",
                bg=bg,
                fg=fg,
                state=state,
                font=("Segoe UI", 11),
                command=lambda s=start, e=end: self._select_time_slot(s, e)
            )

            btn.grid(row=row, column=col, padx=6, pady=6)

            col += 1
            if col == 3:
                col = 0
                row += 1


    def _select_time_slot(self, start, end):
        self.selected_slot = (start, end)
        for btn in self.time_frame.winfo_children():
            if btn["text"] == start:
                btn.configure(bg="#2563eb", fg="white")
            elif btn["state"] == "normal":
                btn.configure(bg="#e8f0ff", fg="#2563eb")
            
        # Update visual table map to reflect reservations overlapping this slot
        date_txt = self.date_entry.get().strip()
        date_iso = None
        try:
            parsed = datetime.strptime(date_txt, "%d-%m-%Y")
            date_iso = parsed.strftime("%Y-%m-%d")
            today = datetime.now().date()
            if parsed.date() <= today:
                # invalid date for reservation: show blue inline hint and do not attempt map update
                self.slot_hint_label.config(text="Please choose a valid date (from tomorrow onwards)", foreground="#2563eb")
                
                return
        except Exception:
            date_iso = None

        if date_iso:
            # redraw map marking tables reserved at this slot
            self._draw_table_map(selected_date_iso=date_iso, selected_start=start, selected_end=end)

            # show hold-until hint if next reservation shortens the slot
            # choose table to check: first selected table (map-driven selection only)
            table_for_check = None
            if getattr(self, "selected_tables", None) and len(self.selected_tables) > 0:
                table_for_check = sorted(self.selected_tables)[0]

            if table_for_check:
                try:
                    max_end = self.db.max_available_end_time(table_for_check, date_iso, start)
                    computed_end = (datetime.strptime(start, "%H:%M") + timedelta(hours=SLOT_DURATION)).strftime("%H:%M")
                    if max_end and computed_end > max_end:
                        self.slot_hint_label.config(text=f"Table available only until {max_end} (can hold until then)")
                    else:
                        self.slot_hint_label.config(text="")
                except Exception:
                    self.slot_hint_label.config(text="")
        

    # LOAD TABLES & MAP
    def _load_tables(self):
        tables = self.db.list_tables()
        try:
            # runtime debug: print table count and sample rows to console
            print(f"DEBUG: ReservationWindow._load_tables: fetched {len(tables)} tables from {getattr(self.db, 'db_file', DB_FILE)}")
            if tables:
                print("DEBUG sample tables:", tables[:15])
        except Exception:
            pass
        display_values = []
        display_values_free = []
        self.table_map = {}
        self.table_label_by_no = {}
        self.tables_list = []
        self.tables_by_no = {}
        self.table_status_by_no = {}

        for table_no,capacity, view, location in tables:
            try:
                tno = int(table_no)
            except Exception:
                tno = table_no
            # Reservation state is determined per-date/slot in _draw_table_map.
            is_reserved = False
            label = f"{tno} (cap {capacity})"
            display_values.append(label)
            self.table_status_by_no[tno] = 0
            self.tables_list.append((tno,capacity, view, location))
            self.tables_by_no[tno] = (tno,capacity, view, location)
            if not is_reserved:
                display_values_free.append(label)
                self.table_map[label] = tno
            # keep reverse mapping for map -> label updates
            self.table_label_by_no[tno] = label

        # Update instruction label to default message
        
        self.table_choice_label.config(text="Choose a table from the table map")

        # If no tables found in DB, render a clear canvas diagnostic message
        if not tables:
            try:
                # clear canvas and show centered message
                try:
                    self.table_canvas.delete("all")
                    cw = max(420, int(self.table_canvas.winfo_width() or 420))
                    ch = max(320, int(self.table_canvas.winfo_height() or 320))
                    mid_x = int(cw / 2)
                    mid_y = int(ch / 2)
                    self.table_canvas.create_text(mid_x, mid_y-10, text=f"No tables in DB ({getattr(self.db, 'db_file', DB_FILE)})", font=(None, 13, "bold"), fill="#b22")
                    self.table_canvas.create_text(mid_x, mid_y+10, text="Ensure TABLE_RESTAURANT exists and contains rows.", font=(None, 10), fill="#333")
                except Exception:
                    pass
            except Exception:
                pass
            # still call _draw_table_map to keep behavior consistent
            try:
                self._draw_table_map()
            except Exception:
                pass
            return

        if getattr(self, "client_data", None):
            self.client_first.insert(0, self.client_data.get("first_name", ""))
            self.client_last.insert(0, self.client_data.get("last_name", ""))
            self.client_email.insert(0, self.client_data.get("email", ""))
            self.client_phone.insert(0, self.client_data.get("phone", ""))

            self.client_first.configure(state="disabled")
            self.client_last.configure(state="disabled")
            self.client_email.configure(state="disabled")
            self.client_phone.configure(state="disabled")
            

        self._draw_table_map()

    def _check_capacity(self):
        """Check selected table(s) capacity against entered people number and
        show an inline hint if capacity is insufficient.
        """
        self.people_hint_label.config(text="")
        
        # parse people count
        people_txt = (getattr(self, "people_entry", None) and self.people_entry.get().strip()) or ""
        people_number = int(people_txt)
        if people_number <= 0:
            return
        
        # if no selected tables, nothing to compare against
        if not getattr(self, "selected_tables", None) or len(self.selected_tables) == 0:
            return

        # compute total capacity of selected tables
        total_cap = 0
        for t in sorted(self.selected_tables):
            try:
                tbl = self.tables_by_no.get(t)
                if tbl:
                    cap = int(tbl[1])
                else:
                    cap = 0
            except Exception:
                cap = 0
            total_cap += cap

        if people_number > total_cap:    
            self.people_hint_label.config(text=f"Not enough capacity: selected table(s) total {total_cap}, required {people_number}")
            
        else:
            self.people_hint_label.config(text="")
            

    def _draw_table_map(self, selected_date_iso=None, selected_start=None, selected_end=None):
        self.table_canvas.delete("all")
        self.table_canvas_items.clear()

        if not getattr(self, "tables_list", None):
            return

        # Base layout coordinates/sizes designed for a larger canvas
        # Increase base size so spacing changes feel consistent on resize
        base_w, base_h = 960.0, 920.0
        layout = {
            1: {"pos": (60, 50),  "type": "rect",   "w": 78, "h": 44},
            2: {"pos": (200, 48), "type": "rect",   "w": 72, "h": 42},
            3: {"pos": (340, 54), "type": "rect",   "w": 78, "h": 44},
            4: {"pos": (140, 142),"type": "round",  "r": 30},
            5: {"pos": (280, 132),"type": "square", "w": 56, "h": 56},
            6: {"pos": (40, 240), "type": "round",  "r": 28},
            7: {"pos": (170, 230),"type": "round",  "r": 30},
            8: {"pos": (300, 230),"type": "rect",   "w": 100, "h": 42},
            9: {"pos": (110, 310),"type": "rect",   "w": 110, "h": 42},
            10:{"pos": (330, 310),"type": "round",  "r": 26},
        }

        # If parameters weren't passed, try to derive them from current UI state
        
        if selected_date_iso is None:
            date_txt = getattr(self, "date_entry", None) and self.date_entry.get().strip()
            if date_txt:
                try:
                    parsed = datetime.strptime(date_txt, "%d-%m-%Y")
                    if parsed.date() > datetime.now().date():
                        selected_date_iso = parsed.strftime("%Y-%m-%d")
                except Exception:
                    selected_date_iso = None
        if (selected_start is None or selected_end is None) and getattr(self, "selected_slot", None):
            selected_start, selected_end = self.selected_slot
                
        
        reserved_set = set()
        if selected_date_iso and selected_start and selected_end:
            try:
                reserved_set = self.db.reserved_tables_for_timeslot(selected_date_iso, selected_start, selected_end)
            except Exception:
                reserved_set = set()
       
        try:
            cw = float(self.table_canvas.winfo_width())
            ch = float(self.table_canvas.winfo_height())
            # If the widget hasn't been laid out yet, winfo_width/height can be tiny (1).
            # In that case use the base design size so the map initially appears at a reasonable scale.
            if cw < (base_w * 0.4):
                cw = base_w
            if ch < (base_h * 0.4):
                ch = base_h
        except Exception:
            cw, ch = base_w, base_h

        sx = max(0.01, cw / base_w)
        sy = max(0.01, ch / base_h)
        smin = min(sx, sy)

        # ensure a positions grid exists (5x5) for placing tables
        # Provide a less rigid, more organic layout (not strict lines)
        try:
            positions = [
                (80, 60), (280, 50), (480, 60), (680, 50), (880, 60),
                (60, 220), (260, 240), (480, 220), (680, 240), (900, 220),
                (120, 380), (320, 360), (520, 380), (720, 360), (920, 380),
                (60, 540), (260, 560), (480, 540), (700, 560), (920, 540),
                (140, 700), (340, 700), (540, 700), (740, 700), (940, 700),
            ]
        except Exception:
            positions = []

        placed = {}
        try:
            total_slots = len(positions)
            slot_indices = list(range(1, total_slots + 1))

            loc_choice = (getattr(self, 'location_filter_var', None) and self.location_filter_var.get()) or 'Μέσα'
            # helper to normalize location strings
            def norm(s):
                try:
                    return str(s or '').strip().lower()
                except Exception:
                    return ''

            def loc_of(tbl):
                try:
                    if tbl is None:
                        return ''
                    if len(tbl) > 4:
                        return norm(tbl[4])
                    if len(tbl) > 3:
                        return norm(tbl[3])
                    return ''
                except Exception:
                    return ''

            loc_norm = norm(loc_choice)
            inside_norm = norm('Μέσα')
            outside_norm = norm('Έξω')
            bar_norm = norm('Μπαρ')

            if loc_norm == inside_norm:
                def is_inside_location(tbl):
                    try:
                        return loc_of(tbl) == inside_norm
                    except Exception:
                        return False

                inside_tables = [t for t in self.tables_list if is_inside_location(t)]
                inside_tables = sorted(inside_tables, key=lambda x: int(x[0]))[:15]

                # position pools
                left_pos = [i for i in slot_indices if ((i-1) % 5) == 0]
                right_pos = [i for i in slot_indices if ((i-1) % 5) == 4]
                center_pos = [i for i in slot_indices if ((i-1) % 5) in (1,2,3)]
                window_pool = [i for i in slot_indices if ((i-1) // 5) == 0]
                garden_pool = [i for i in slot_indices if ((i-1) // 5) == 4]

                # Prefer tables whose `view` contains '-' to be placed in the very center
                def has_hyphen_view(tbl):
                    try:
                        v = str(tbl[3] or '').strip()
                        return v in ('-', '—', '–')
                    except Exception:
                        return False

                center_tables = [t for t in inside_tables if has_hyphen_view(t)]
                other_tables = [t for t in inside_tables if t not in center_tables]
                ordered = center_tables + other_tables

                # center_pool: best center-first ordering (center then immediate neighbors)
                center_pool = [13, 8, 12, 14, 18]

                used = set()
                for tbl in ordered:
                    view = norm(tbl[3])
                    assigned = None
                    if tbl in center_tables:
                        # try center-first pool, then center_pos, then fallback
                        for p in center_pool + center_pos + left_pos + right_pos + window_pool + garden_pool:
                            if p not in used:
                                assigned = p
                                break
                    else:
                        # non-center tables: surround the center, prefer middle columns then sides
                        # window/garden preferences still honored when keywords present
                        if 'Παράθυρο' in view or 'παραθυρο' in view:
                            pools = window_pool + left_pos + center_pos + right_pos
                        elif 'Κήπος' in view or 'κηπος' in view or 'κήπος' in view:
                            pools = garden_pool + right_pos + center_pos + left_pos
                        elif 'Μπαρ' in view:
                            pools = center_pos + left_pos + right_pos
                        else:
                            pools = center_pos + left_pos + right_pos

                        for p in pools + slot_indices:
                            if p not in used:
                                assigned = p
                                break

                    if assigned is None:
                        for p in slot_indices:
                            if p not in used:
                                assigned = p
                                break

                    if assigned:
                        placed[assigned] = tbl
                        used.add(assigned)
            else:
                placed = {}
        except Exception:
            placed = {}

        # decide how many slots we will render (use positions grid)
        total_slots = len(positions) or 25
        loc_choice = (getattr(self, 'location_filter_var', None) and self.location_filter_var.get()) or 'Μέσα'
        loc_norm = norm(loc_choice)
        try:
            total_tables = len(self.tables_list)
            total_inside = len([t for t in self.tables_list if loc_of(t) == inside_norm])
        except Exception:
            total_tables = 0
            total_inside = 0
        if loc_norm == outside_norm:
            try:
                mid_x = int(cw / 2)
                mid_y = int(ch / 2)
                self.table_canvas.create_text(mid_x, mid_y, text="Όλα τα εξωτερικά τραπέζια έχουν ίδιο view γι'αυτό δεν εμφανίζονται.", font=(None, 12), fill="#333")
            except Exception:
                pass
            return

        if loc_norm == inside_norm and not placed:
                mid_x = int(cw / 2)
                mid_y = int(ch / 2)
                self.table_canvas.create_text(mid_x, mid_y-10, text=f"No tables placed. total_tables={total_tables}, inside={total_inside}", font=(None, 11), fill="#b22")
                self.table_canvas.create_text(mid_x, mid_y+10, text="Check table `location` values or adjust placement rules.", font=(None, 10), fill="#666")

        for slot_no in range(1, total_slots + 1):
            try:
                bx, by = positions[slot_no - 1]
            except Exception:
                bx, by = (40 + ((slot_no - 1) % 5) * 80, 48 + ((slot_no - 1) // 5) * 92)
            cx = int(bx * sx)
            cy = int(by * sy)

            # Prefer placed mapping; fallback to previous tables_by_no mapping
            # But avoid drawing the same table twice: if a table exists in
            # `placed` at a different slot, do not draw it again
            row = None
            if slot_no in placed:
                row = placed.get(slot_no)
            else:
                candidate = self.tables_by_no.get(slot_no)
                if candidate:
                    if loc_norm == inside_norm and loc_of(candidate) != inside_norm:
                     candidate = None

                if candidate:
                    try:
                        cand_tno = candidate[0]
                        already_placed_elsewhere = any(
                            (v and v[0] == cand_tno) for k, v in placed.items() if k != slot_no
                        )
                    except Exception:
                        already_placed_elsewhere = False
                    if not already_placed_elsewhere:
                        row = candidate

            # If no real table is mapped to this slot, skip drawing placeholders
            if not row:
                continue
            table_no,capacity, view, location = row
            try:
                loc_lower = (location or "").strip().lower()
            except Exception:
                loc_lower = ""
            # hide bar tables from reservation map
            if "μπαρ" in loc_lower:
                is_real = False
            else:
                is_real = True
            try:
                cap = int(capacity) if capacity is not None else 2
            except Exception:
                cap = 2
            if cap <= 2:
                meta_type = "round"
                r = max(12, 10 + cap * 4)
            elif cap <= 4:
                meta_type = "rect"
                w = max(64, 64 + (cap - 2) * 18)
                h = max(36, 36 + (cap - 2) * 10)
            elif cap <= 6:
                meta_type = "rect"
                w, h = 120, 50
            else:
                meta_type = "rect"
                w, h = 160, 60

            tag = f"table_{table_no if table_no is not None else 'slot'+str(slot_no)}"
            fill = "#ffffff" if is_real else "#f8fafc"
            outline = "#2f3a49" if is_real else "#cbd5e1"
            reserved_flag = False
            if selected_date_iso and selected_start and selected_end:
                if is_real and table_no in reserved_set:
                    reserved_flag = True
            if is_real and reserved_flag:
                fill = "#f0f2f5"
                outline = "#bfc6cd"

            is_selected_local = False
            try:
                if getattr(self, 'selected_tables', None) and table_no in self.selected_tables:
                    is_selected_local = True
            except Exception:
                is_selected_local = False
            if is_selected_local and not reserved_flag:
                outline = "#2e7d32"
                fill = "#ffffff"

            items = []
            shadow_off = max(1, int(3 * smin))

            if meta_type == "round":
                r_s = max(25, int(r * smin * 1.25))
                x1, y1 = cx - r_s, cy - r_s
                x2, y2 = cx + r_s, cy + r_s
                shadow = self.table_canvas.create_oval(x1+shadow_off, y1+shadow_off, x2+shadow_off, y2+shadow_off, fill="#e9edf2", outline="", tags=(tag,))
                stroke_w = 4 if is_selected_local else max(2, int(3 * smin))
                oval = self.table_canvas.create_oval(x1, y1, x2, y2, fill=fill, outline=outline, width=stroke_w, tags=(tag,))
                items.extend([oval])
                label_y = cy - max(2, int(4 * smin))
                cap_y = cy + int(r_s * 0.6)
            else:
                w_s = max(10, int(w * sx))
                h_s = max(8, int(h * sy))
                x1, y1 = cx - w_s/2, cy - h_s/2
                x2, y2 = cx + w_s/2, cy + h_s/2
                shadow = self.table_canvas.create_rectangle(x1+shadow_off, y1+shadow_off, x2+shadow_off, y2+shadow_off, fill="#e9edf2", outline="", tags=(tag,))
                stroke_w = 4 if is_selected_local else max(2, int(3 * smin))
                rect = self.table_canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline=outline, width=stroke_w, tags=(tag,))
                items.extend([rect])
                label_y = cy - max(6, int(6 * smin))
                cap_y = cy + max(8, int(12 * smin))

            display_no = table_no if table_no is not None else slot_no
            num_size = max(8, int(10 * smin))
            cap_size = max(7, int(8 * smin))
            try:
                num_txt = self.table_canvas.create_text(cx, label_y, text=str(display_no), font=("Helvetica", num_size, "bold"), tags=(tag,))
                cap_txt = self.table_canvas.create_text(cx, cap_y, text=(f"cap {capacity}" if is_real else "(no table)"), font=("Helvetica", cap_size), fill="#4a5568", tags=(tag,))
            except Exception:
                num_txt = self.table_canvas.create_text(cx, label_y, text=str(display_no), tags=(tag,))
                cap_txt = self.table_canvas.create_text(cx, cap_y, text=(f"cap {capacity}" if is_real else "(no table)"), fill="#4a5568", tags=(tag,))

            items.extend([num_txt, cap_txt, shadow])
            key = table_no if table_no is not None else f"slot_{slot_no}"
            self.table_canvas_items[key] = tuple(items)

            try:
                is_reserved_local = (table_no in getattr(self, 'reserved_tables_set', set())) or (self.table_status_by_no.get(table_no, 0) == 1)
            except Exception:
                is_reserved_local = False
            if is_real and not is_reserved_local:
                def make_cb(tn):
                    return lambda e: self._toggle_table_selection(tn)
                self.table_canvas.tag_bind(tag, "<Button-1>", make_cb(table_no))

    def _toggle_table_selection(self, table_no):
        if table_no not in self.table_canvas_items:
            return
        
        if self.table_status_by_no.get(table_no, 0) == 1:
            return
        
        items = self.table_canvas_items[table_no]
        rect = items[0]
        txt = items[1] if len(items) > 1 else None
        cap_txt = items[2] if len(items) > 2 else None

        if table_no in self.selected_tables:
            self.selected_tables.remove(table_no)           
            self.table_canvas.itemconfig(rect, fill="#ffffff", outline="#3b4252", width=1)
            
            if txt:
                self.table_canvas.itemconfig(txt, fill="#000000")
    
            if cap_txt:
                self.table_canvas.itemconfig(cap_txt, fill="#4a5568")
            
        else:
            self.selected_tables.add(table_no)
            self.table_canvas.itemconfig(rect, fill="#ffffff", outline="#2e7d32", width=3)
            
            if txt:    
                self.table_canvas.itemconfig(txt, fill="#000000")
                
            if cap_txt:
                self.table_canvas.itemconfig(cap_txt, fill="#000000")
       
        if getattr(self, "selected_tables", None) and len(self.selected_tables) == 1:
            t = sorted(self.selected_tables)[0]
            label = self.table_label_by_no.get(t)
            if label:
                self.table_choice_label.config(text=label)
        elif getattr(self, "selected_tables", None) and len(self.selected_tables) > 1:
            self.table_choice_label.config(text="Multiple selected")
        else:
            # no selection on map, restore instruction
            self.table_choice_label.config(text="Choose a table from the table map")
    

        self._check_capacity()
       
        self._load_time_slots()
        

    def _on_back(self):
        
        if callable(self.return_callback):
            self.destroy()
            self.return_callback()
            return
        
        self.destroy()

    def save_reservation(self):
        client_data = {
            "first_name": self.client_first.get().strip(),
            "last_name": self.client_last.get().strip(),
            "email": self.client_email.get().strip(),
            "phone": self.client_phone.get().strip(),
        }
        # Validate phone and email 
        phone = client_data.get("phone", "")
        try:
            if not re.fullmatch(r"69\d{8}", phone):
                messagebox.showwarning("Invalid Phone", "Please enter a valid phone number.")
                return
        except Exception:
            messagebox.showwarning("Invalid Phone", "Please enter a valid phone number.")
            return

        email = client_data.get("email", "")
        email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
        try:
            if email and not re.fullmatch(email_pattern, email):
                messagebox.showwarning("Invalid Email", "Please enter a valid email address.")
                return
        except Exception:
            messagebox.showwarning("Invalid Email", "Please enter a valid email address.")
            return
        
        reserv_date = self.date_entry.get().strip()      # DD-MM-YYYY
        # Validate date must be tomorrow or later
        try:
            parsed_date = datetime.strptime(reserv_date, "%d-%m-%Y")
            if parsed_date.date() <= datetime.now().date():
                messagebox.showerror("Invalid date", "Reservations must be made for tomorrow or later.")
                return
        except Exception:
            messagebox.showerror("Invalid date", "Date must be in DD-MM-YYYY format")
            return
        people_txt = self.people_entry.get().strip()

        if (
            not all(client_data.values())
            or not reserv_date
            or not people_txt
        ):
            messagebox.showerror("Missing data", "Please fill in all fields.")
            return

        try:
            people_number = int(people_txt)
        except ValueError:
            messagebox.showerror("Invalid value", "Number of people must be an integer.")
            return

        # Determine start time from the selected slot
        if getattr(self, "selected_slot", None):
            start_time = self.selected_slot[0]

            reserv_date_iso = parsed_date.strftime("%Y-%m-%d")

            table_for_check = None
            if getattr(self, "selected_tables", None) and len(self.selected_tables) > 0:
                table_for_check = sorted(self.selected_tables)[0]

            if table_for_check:
                status = self.db.slot_availability_status(
                    table_no=table_for_check,
                    reserv_date=reserv_date_iso,
                    start_time=start_time
                )
            else:
                status = "full"  

            start_dt = datetime.strptime(start_time, "%H:%M")

            if status == "partial":
                end_dt = start_dt + timedelta(hours=1)
            else:
                end_dt = start_dt + timedelta(hours=2)

            end_time = end_dt.strftime("%H:%M")

        else:
            messagebox.showerror("Missing time", "Please select a time slot.")
            return


        if getattr(self, "selected_tables", None) and len(self.selected_tables) > 0:
            for t in self.selected_tables:
                if self.table_status_by_no.get(t, 0) == 1:
                    messagebox.showerror("Table unavailable", f"Table {t} is reserved and cannot be selected.")
                    return
            selected = sorted(self.selected_tables)
            table_arg = selected if len(selected) > 1 else selected[0]
        else:
            # No map selection. Automatically pick an available table matching capacity.
            reserv_date_iso = parsed_date.strftime("%Y-%m-%d")
            try:
                # respect the reservation area filter (Μέσα / Έξω)
                loc_choice = (getattr(self, 'location_filter_var', None) and self.location_filter_var.get()) or 'Μέσα'
                loc_choice_norm = (str(loc_choice or '').strip().lower())
                candidates = []
                for t in self.tables_list:
                    try:
                        tno = int(t[0])
                    except Exception:
                        continue
                    # extract location and capacity from the tuple (tno, capacity, view, location)
                    try:
                        tbl_loc = str(t[3] or '').strip()
                    except Exception:
                        tbl_loc = ''
                    # normalize and compare locations
                    if tbl_loc.lower() != loc_choice_norm:
                        continue
                    # skip tables currently marked unavailable (status)
                    if self.table_status_by_no.get(tno, 0) == 1:
                        continue
                    try:
                        cap = int(t[1]) if t[1] is not None else 0
                    except Exception:
                        cap = 0
                    # check availability for the selected slot/date
                    try:
                        if self.db.table_is_available_for_slot(tno, reserv_date_iso, start_time, end_time):
                            candidates.append((tno, cap))
                    except Exception:
                        # if availability check fails, skip this table
                        continue

                if not candidates:
                    messagebox.showerror("No available table", "No table is available for the selected date/time.")
                    return

                # prefer tables with capacity >= people_number and <= people_number+2
                good = [c for c in candidates if c[1] >= people_number and c[1] <= (people_number + 2)]
                if not good:
                    # fallback: any table with capacity >= people_number
                    good = [c for c in candidates if c[1] >= people_number]

                if not good:
                    messagebox.showerror("No suitable table", "No table with sufficient capacity is available for the selected date/time.")
                    return

                # choose table(s) with the smallest capacity that fits
                min_cap = min(c[1] for c in good)
                best = [c[0] for c in good if c[1] == min_cap]
                chosen = random.choice(best)
                selected = [chosen]
                table_arg = chosen
            except Exception:
                messagebox.showerror("Selection error", "Could not automatically select a table.")
                return

        # Check total capacity of selected tables before proceeding
        try:
            total_cap = 0
            for t in sorted(selected):
                tbl = self.tables_by_no.get(t)
                if tbl:
                    total_cap += int(tbl[1])
            if people_number > total_cap:
                messagebox.showerror("Not enough capacity", f"Selected table(s) total capacity {total_cap} is less than required {people_number}.")
                return
        except Exception:
            pass

        # Adjust end_time if a next reservation limits availability
        # If multiple tables selected, use the first table for this check
        try:
            table_for_check = sorted(selected)[0]
            max_end = self.db.max_available_end_time(
                table_no=table_for_check,
                reserv_date=convert_ddmmyyyy_to_iso(reserv_date),
                requested_start=start_time,
            )
            if max_end and end_time > max_end:
                messagebox.showinfo(
                    "Limited availability",
                    f"Table is only available until {max_end}; end time adjusted."
                )
                end_time = max_end
        except Exception:
            pass

        try:
            waiter_id = self.waiter["waiter_id"] if self.waiter else None
            reserv_no = self.db.create_reservation(
                waiter_id=waiter_id,
                client_data=client_data,
                reserv_date=reserv_date,
                start_time=start_time,
                end_time=end_time,
                people_number=people_number,
                table_no=table_arg,
            )
        except Exception as e:
            messagebox.showerror("Error", f"Could not save reservation: {e}")
            return

        messagebox.showinfo("Success", f"Reservation {reserv_no} created successfully.")
        
        if callable(getattr(self, "return_callback", None)):
            self.destroy()
            self.return_callback()
            return
        
        self.destroy()


class ReservationsManager(tk.Toplevel):
    def __init__(self, master, db: Database, return_callback=None):
        super().__init__(master)
        self.db = db
        self.return_callback = return_callback
        self.title("Manage Reservations")
        self.state("zoomed")
        
        self.minsize(800, 500)

        toolbar = ttk.Frame(self, padding=6)
        toolbar.pack(fill="x")
        ttk.Button(toolbar, text="Refresh", command=self.populate, style="Accent.TButton").pack(side="left")
        ttk.Button(toolbar, text="Delete Selected", command=self.delete_selected, style="Accent.TButton").pack(side="left", padx=6)
        ttk.Button(toolbar, text="Close", command=self._close, style="Accent.TButton").pack(side="right")

        cols = ("reserv_no", "date", "start_time", "end_time", "people", "client", "phone", "tables")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        for c in cols:
            self.tree.heading(c, text=c.replace("_", " ").title())
            self.tree.column(c, width=120, anchor="center")
        self.tree.pack(expand=True, fill="both", pady=8, padx=8)

        self.populate()

    def populate(self):
        for r in self.tree.get_children():
            self.tree.delete(r)
        try:
            rows = self.db.list_reservations()
        except Exception as e:
            messagebox.showerror("DB Error", f"Could not load reservations: {e}")
            return
        # Show only reservations from today onwards
        today = datetime.now().date()
        filtered_rows = []
        for row in rows:
            try:
                dtxt = row[1]
                if not dtxt:
                    continue
                # try ISO YYYY-MM-DD first
                try:
                    d = datetime.strptime(dtxt, "%Y-%m-%d").date()
                except Exception:
                    # try DD-MM-YYYY fallback
                    try:
                        d = datetime.strptime(dtxt, "%d-%m-%Y").date()
                    except Exception:
                        d = None
                if d is None or d >= today:
                    filtered_rows.append(row)
            except Exception:
                filtered_rows.append(row)

        for row in filtered_rows:
            # row: reserv_no, date, start_time, end_time, people_number, first, last, phone, tables
            reserv_no = row[0]
            date = row[1]
            start = row[2]
            end = row[3]
            people = row[4]
            client = f"{row[5] or ''} {row[6] or ''}".strip()
            phone = row[7]
            tables = row[8]
            self.tree.insert("", "end", values=(reserv_no, date, start, end, people, client, phone, tables))

    def delete_selected(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("No selection", "Please select a reservation to delete.")
            return
        try:
            vals = self.tree.item(sel[0])['values']
            reserv_no = vals[0]
        except Exception:
            messagebox.showerror("Selection Error", "Could not determine reservation id.")
            return
        if not messagebox.askyesno("Confirm Delete", f"Delete reservation {reserv_no}? This cannot be undone."):
            return
        try:
            ok = self.db.delete_reservation(reserv_no)
        except Exception as e:
            messagebox.showerror("DB Error", f"Could not delete reservation: {e}")
            return
        if ok:
            messagebox.showinfo("Deleted", f"Reservation {reserv_no} deleted.")
            self.populate()
        else:
            messagebox.showerror("Error", "Failed to delete reservation.")

    def _close(self):
        try:
            self.destroy()
            if callable(self.return_callback):
                self.return_callback()
        except Exception:
                self.destroy()
            
class OrdersWindow(tk.Toplevel):
    def __init__(self, master, db: Database, return_callback=None):
        super().__init__(master)
        self.master = master
        self.db = db
        self.return_callback = return_callback

        self.title("All Orders")
        self.state("zoomed")
        
        self.minsize(900, 500)

        frame = ttk.Frame(self, padding=10)
        frame.pack(expand=True, fill="both")

        toolbar = ttk.Frame(frame)
        toolbar.pack(fill="x")
        ttk.Button(toolbar, text="Refresh", command=self.populate, style="Accent.TButton").pack(side="left")
        ttk.Button(toolbar, text="Mark Paid", command=self._mark_paid, style="Accent.TButton").pack(side="left", padx=6)
        # kind filter
        ttk.Label(toolbar, text="Kind:").pack(side="left", padx=(10,2))
        self.kind_var = tk.StringVar(value="all")
        kind_cb = ttk.Combobox(toolbar, textvariable=self.kind_var, values=("all", "in_place", "online"), width=10, state="readonly")
        kind_cb.pack(side="left")
        kind_cb.bind("<<ComboboxSelected>>", lambda e: self.populate())
        ttk.Button(toolbar, text="Close", command=self._close, style="Accent.TButton").pack(side="right")

        cols = ("order_id", "datetime", "kind", "table_no", "client_id", "address", "estimated_time", "comments", "total_amount", "paid")
        self.tree = ttk.Treeview(frame, columns=cols, show="headings")
        for c in cols:
            self.tree.heading(c, text=c.replace("_", " ").title())
            self.tree.column(c, width=120, anchor="center")
        self.tree.pack(expand=True, fill="both", pady=10)

        self.populate()

    def _mark_paid(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showwarning("No selection", "Please select an order to mark as paid.")
            return
        try:
            vals = self.tree.item(sel[0])['values']
            order_id = vals[0]
        except Exception:
            messagebox.showerror("Selection Error", "Could not determine selected order id.")
            return

        try:
            tip = simpledialog.askfloat("Tip", "Enter tip amount (0 for none):", minvalue=0.0, initialvalue=0.0)
        except Exception:
            tip = None

        if tip is None:
            # Ask whether to proceed without tip or cancel
            proceed = messagebox.askyesno("No Tip Entered", "No tip was entered. Mark order as paid without tip?")
            if not proceed:
                return
            tip_amount = None
        else:
            tip_amount = float(tip)

        ok = False
        try:
            paid_by_card = messagebox.askyesno("Payment Method", "Was the payment made by card? (Yes = Card, No = Cash)")
            payment_method = "card" if paid_by_card else "cash"
            ok = self.db.mark_order_paid(order_id, tip_amount, payment_method=payment_method)
        except Exception as e:
            messagebox.showerror("DB Error", f"Could not mark order paid: {e}")
            return

        if ok:
            messagebox.showinfo("Success", f"Order {order_id} marked as paid.")    
            self.populate()
            
        else:
            messagebox.showerror("Error", "Failed to mark order as paid.")

    def populate(self):
        for r in self.tree.get_children():
            self.tree.delete(r)
        try:
            rows = self.db.list_all_orders()
        except Exception as e:
            messagebox.showerror("DB Error", f"Could not load orders: {e}")
            return
        # Show only orders from the same day
        today = datetime.now().date()
        filtered_rows = []
        for row in rows:
            try:
                dtxt = row[1]
                if not dtxt:
                    continue
                # try ISO / fromisoformat first
                try:
                    d = datetime.fromisoformat(dtxt).date()
                except Exception:
                    # try common fallbacks
                    try:
                        d = datetime.strptime(dtxt, "%Y-%m-%d %H:%M:%S").date()
                    except Exception:
                        try:
                            d = datetime.strptime(dtxt, "%Y-%m-%d").date()
                        except Exception:
                            try:
                                d = datetime.strptime(dtxt, "%d-%m-%Y %H:%M:%S").date()
                            except Exception:
                                try:
                                    d = datetime.strptime(dtxt, "%d-%m-%Y").date()
                                except Exception:
                                    d = None
                if d == today:
                    filtered_rows.append(row)
            except Exception:
                # on parse error, skip the row so only clear same-day orders show
                continue

        if not rows:

            conn = self.db._get_conn()
            cur = conn.cursor()
            counts = {}
            for t in ("ORDERS", "IN_PLACE_ORDER", "ONLINE_ORDER"):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    counts[t] = cur.fetchone()[0]
                except Exception as ex:
                    counts[t] = f"ERR: {ex}"
            conn.close()
            message = "Database counts:\n" + "\n".join([f"{k}: {v}" for k, v in counts.items()])
            messagebox.showinfo("DB Diagnostic", message)
            
        # apply kind filter if set (operate on same-day subset)
        kind = (self.kind_var.get() if getattr(self, 'kind_var', None) else 'all')
        filtered = []
        for row in filtered_rows:
            if kind == 'all' or (len(row) >= 3 and row[2] == kind):
                filtered.append(row)

        for row in filtered:
            vals = list(row)
            orig_len = len(vals)
            orig_paid = None
            if orig_len >= 9:
                try:
                    orig_paid = vals[8]
                except Exception:
                    orig_paid = None

            if len(vals) < 10:
                vals += [None] * (10 - len(vals))
           
            if orig_paid is not None:
                vals[9] = orig_paid
            
            try:
                conn = self.db._get_conn()
                cur = conn.cursor()
                try:
                    cur.execute("SELECT SUM(total_amount) FROM RECEIPT WHERE order_id = ?", (vals[0],))
                    total = cur.fetchone()[0]
                except Exception:
                    try:
                        cur.execute("SELECT SUM(net_price * quantity) FROM RECEIPT WHERE order_id = ?", (vals[0],))
                        total = cur.fetchone()[0]
                    except Exception:
                        total = None
                
                conn.close()
                
                if total is None:
                    total = 0.0
                vals[8] = f"{float(total):.2f}"
            except Exception:
                try:
                    vals[8] = f"{float(vals[8]) if vals[8] is not None else 0.0:.2f}"
                except Exception:
                    vals[8] = "0.00"
           
            if vals[7] is not None and str(vals[7]).strip() == "":
                vals[7] = None
            

            self.tree.insert("", "end", values=vals)
        count = len(filtered)
        
        if hasattr(self, '_status_lbl'):
            self._status_lbl.config(text=f"{count} orders shown")
        else:
            self._status_lbl = ttk.Label(self, text=f"{count} orders shown")
            self._status_lbl.pack(side="bottom", fill="x")    

    def _close(self):
        try:
            self.destroy()
            if self.return_callback:
                self.return_callback()
            else:    
                self.master.deiconify()
                
        except Exception:
            self.destroy()
            
        return

class OrderWindow(tk.Toplevel):
    def __init__(self, master, db: Database, waiter: dict, return_callback=None):
        super().__init__(master)
        self.db = db
        self.waiter = waiter
        self.return_callback = return_callback

        # store after() id so we can cancel scheduled callbacks when window closes
        self._after_id = None
        # cancel scheduled callbacks when the widget is destroyed
        
        self.bind("<Destroy>", lambda e: self._cancel_after())
 

        self.title("Create Table Order")
        self.state("zoomed")
        self.bind("<Escape>", lambda e: self.state("normal"))
        self.minsize(1024, 768)

        self.resizable(True, True)

        self.items_in_order = {} 

        ttk.Label(self, text="Create Table Order", font=("Arial", 16, "bold")).pack(
            pady=10
        )

        top_frame = ttk.Frame(self, padding=10)
        top_frame.pack(fill="x")

        # Current order time
        self.order_time_label = ttk.Label(top_frame, text=f"Time: {datetime.now().strftime('%H:%M')}")
        self.order_time_label.pack(side="left", padx=(0,12))
   
        # start live updates of the time label (every 60 seconds)
        self._update_order_time()
  

        ttk.Label(top_frame, text="Table:").pack(side="left")
        self.table_combo = ttk.Combobox(top_frame, state="readonly", width=20)
        self.table_combo.pack(side="left", padx=5)
        self._load_tables()

        main = ttk.Frame(self, padding=10)
        main.pack(fill="both", expand=True)

        # Menu items
        menu_frame = ttk.Frame(main)
        menu_frame.pack(side="left", fill="both", expand=True)

        ttk.Label(menu_frame, text="Menu Items").pack()
        self.menu_tree = ttk.Treeview(
            menu_frame,
            columns=("id", "description", "price"),
            show="headings",
            height=15,
        )
        self.menu_tree.heading("id", text="ID")
        self.menu_tree.heading("description", text="Description")
        self.menu_tree.heading("price", text="Price")
        self.menu_tree.column("id", width=60, anchor="center")
        self.menu_tree.column("description", width=220)
        self.menu_tree.column("price", width=80, anchor="e")
        self.menu_tree.pack(fill="both", expand=True)

        self._load_menu_items()

        qty_frame = ttk.Frame(menu_frame)
        qty_frame.pack(fill="x", pady=5)
        ttk.Label(qty_frame, text="Quantity:").pack(side="left")
        self.qty_entry = ttk.Entry(qty_frame, width=5)
        self.qty_entry.insert(0, "1")
        self.qty_entry.pack(side="left", padx=5)
        ttk.Button(qty_frame, text="Add to Order", command=self.add_item, style="Accent.TButton").pack(
            side="left", padx=5
        )

        # Order summary
        order_frame = ttk.Frame(main)
        order_frame.pack(side="right", fill="both", expand=True)

        ttk.Label(order_frame, text="Current Order").pack()
        self.order_list = tk.Listbox(order_frame, height=15)
        self.order_list.pack(fill="both", expand=True)

        self.total_label = ttk.Label(order_frame, text="Total: 0.00")
        self.total_label.pack(pady=5)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Save Order", command=self.save_order, style="Accent.TButton").pack(side="left", padx=8)
        ttk.Button(btn_frame, text="Back", command=self._on_back, style="Accent.TButton").pack(side="left", padx=8)

    def _load_tables(self):
        tables = self.db.list_tables()
        display_values = []
        self.table_map = {}
        for table_no,capacity, view, location in tables:
            # Show only the table number in the combobox
            label = str(table_no)
            display_values.append(label)
            self.table_map[label] = table_no
        if display_values:
            self.table_combo["values"] = display_values
            self.table_combo.current(0)
        # Refresh the order time label to current time when loading tables
        
        if hasattr(self, 'order_time_label'):
            self.order_time_label.config(text=f"Time: {datetime.now().strftime('%H:%M')}")


    def _update_order_time(self):
        
        if hasattr(self, 'order_time_label'):
            self.order_time_label.config(text=f"Time: {datetime.now().strftime('%H:%M')}")
    
        try:
            # schedule next update after 60 seconds and keep the id
            if getattr(self, 'winfo_exists', lambda: False)() and self.winfo_exists():
                self._after_id = self.after(60000, self._update_order_time)
        except tk.TclError:
            pass

    def _cancel_after(self):
        
        if getattr(self, '_after_id', None):
            self.after_cancel(self._after_id)
            self._after_id = None
       

    def _load_menu_items(self):
        items = self.db.list_menu_items()
        for item_id, description, net_price, availability, allergens, tax_name in items:
            if availability:
                self.menu_tree.insert(
                    "", "end", values=(item_id, description, f"{net_price:.2f}")
                )

    def add_item(self):
        selected = self.menu_tree.selection()
        if not selected:
            messagebox.showwarning("No selection", "Please select a menu item.")
            return

        try:
            qty = int(self.qty_entry.get())
            if qty <= 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror(
                "Invalid quantity", "Quantity must be a positive integer."
            )
            return

        item_id, desc, price_str = self.menu_tree.item(selected[0])["values"]

        self.items_in_order[item_id] = self.items_in_order.get(item_id, 0) + qty

        self._refresh_order_list()

    def _refresh_order_list(self):
        self.order_list.delete(0, "end")
        total = 0.0

        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        for iid, q in self.items_in_order.items():
            cur.execute(
                "SELECT description, net_price FROM MENU_ITEM WHERE item_id = ?", (iid,)
            )
            desc_db, price_db = cur.fetchone()
            subtotal = q * float(price_db)
            total += subtotal
            self.order_list.insert(
                "end", f"{desc_db} x {q} = {subtotal:.2f}"
            )
        conn.close()

        self.total_label.config(text=f"Total: {total:.2f}")

    def save_order(self):
        table_label = self.table_combo.get()
        if not table_label:
            messagebox.showerror("Missing table", "Please select a table.")
            return
        if not self.items_in_order:
            messagebox.showerror("Empty order", "Please add at least one item.")
            return

        table_no = self.table_map[table_label]
        items_list = [(iid, qty) for iid, qty in self.items_in_order.items()]

        try:
            order_id = self.db.create_in_place_order(
                waiter_id=self.waiter["waiter_id"],
                table_no=table_no,
                items=items_list,
            )
        except Exception as e:
            messagebox.showerror("Error", f"Could not save order: {e}")
            return

        messagebox.showinfo("Success", f"Order {order_id} saved successfully.")
        
        if callable(getattr(self, "return_callback", None)):
            self.destroy()
            self.return_callback()
            return
        
        self.destroy()

    def _on_back(self):
        
        if callable(getattr(self, "return_callback", None)):
            self.destroy()
            self.return_callback()
            return
        self.destroy()


class CustomerUI(ttk.Frame):
    def __init__(self, master, db):
        super().__init__(master, padding=10)
        self.master = master
        self.db = db

        master.title("Customer – Online Portal")
        # open the client window zoomed (full-screen) and allow resizing
        try:
            master.state("zoomed")
        except Exception:
            # fallback for platforms that don't support state("zoomed")
            master.geometry("800x600")
        master.resizable(True, True)

        ttk.Label(self, text="Customer Online Portal", font=("Segoe UI", 14, "bold")).pack(pady=10)

        tk.Button(self, text="Make Reservation", width=25, command=self.online_reservation,
              bg="#eaf4ff", fg="#000000", activebackground="#d4ecff", activeforeground="#000000",
              font=("Segoe UI", 11), relief="raised").pack(pady=10)

        tk.Button(self, text="Order Online", width=25, command=self.online_order,
              bg="#eaf4ff", fg="#000000", activebackground="#d4ecff", activeforeground="#000000",
              font=("Segoe UI", 11), relief="raised").pack(pady=10)

        tk.Button(self, text="Exit", width=25, command=self._exit_to_login,
              bg="#eaf4ff", fg="#000000", activebackground="#d4ecff", activeforeground="#000000",
              font=("Segoe UI", 11), relief="raised").pack(pady=20)

        self.pack()

    def online_reservation(self):    
        self.master.withdraw()
        ReservationWindow(self.master, self.db, waiter=None, return_callback=self._restore)

    def _restore(self):
        self.master.deiconify()
        # Ensure the customer window returns to maximized/full view
        try:
            self.master.state("zoomed")
        except Exception:    
            self.master.geometry("1024x768")


    def _exit_to_login(self):
        self.master.destroy()
        stop_ready_listener()
        
        try:
            # Recreate the login app using the same Database instance
            app = LoginApp(self.db)
            app.mainloop()
        except Exception:
            # If creating LoginApp fails, fallback to opening a simple Tk login
                root = tk.Tk()
                LoginApp(self.db)
                root.mainloop()
            

    def online_order(self):    
        self.master.withdraw()
        CustomerInfoWindow(self.master, self.db, mode="order", return_callback=self._restore)

class OnlineOrderDetailsWindow(tk.Toplevel):
    def __init__(self, master, callback):
        super().__init__(master)
        self.callback = callback  # function to call with form data

        self.title("Order Details")
        self.state("zoomed")
        self.bind("<Escape>", lambda e: self.state("normal"))
        ttk.Label(self, text="Order Details", font=("Arial", 14, "bold")).pack(pady=10)

        ttk.Label(self, text="Order Type:").pack(anchor="w", padx=20)
        self.order_type = ttk.Combobox(self, values=["delivery", "takeaway"], state="readonly")
        self.order_type.current(0)
        self.order_type.pack(fill="x", padx=20, pady=5)
        self.order_type.bind("<<ComboboxSelected>>", self.toggle_address)

        ttk.Label(self, text="Address (delivery only):").pack(anchor="w", padx=20)
        self.address_entry = ttk.Entry(self)
        self.address_entry.pack(fill="x", padx=20, pady=5)
        # travel time display and quick-check
        self.travel_label = ttk.Label(self, text="")
        self.travel_label.pack(anchor="w", padx=20)
        ttk.Button(self, text="Check travel time", command=self.compute_and_show_travel, style="Accent.TButton").pack(padx=20, pady=(0,6))
        # compute travel time when address field loses focus
        self.address_entry.bind("<FocusOut>", lambda e: self.compute_and_show_travel())

        ttk.Label(self, text="Requested time (HH:MM):").pack(anchor="w", padx=20)
        self.time_entry = ttk.Entry(self)
        self.time_entry.insert(0, datetime.now().strftime("%H:%M"))
        self.time_entry.pack(fill="x", padx=20, pady=5)

        ttk.Label(self, text="Comments:").pack(anchor="w", padx=20)
        self.comments_entry = ttk.Entry(self)
        self.comments_entry.pack(fill="x", padx=20, pady=5)

        ttk.Label(self, text="Tip (optional):").pack(anchor="w", padx=20, pady=(8,0))
        self.tip_entry = ttk.Entry(self)
        self.tip_entry.insert(0, "0.00")
        self.tip_entry.pack(fill="x", padx=20, pady=5)

        ttk.Label(self, text="Payment Method:").pack(anchor="w", padx=20, pady=(8,0))
        self.payment_method = ttk.Combobox(self, values=["card", "cash"], state="readonly")
        self.payment_method.current(0)
        self.payment_method.pack(fill="x", padx=20, pady=5)
        self.payment_method.bind("<<ComboboxSelected>>", lambda e: self._toggle_payment_fields())

        self.card_frame = ttk.Frame(self)
        ttk.Label(self.card_frame, text="Card Number:").grid(row=0, column=0, sticky="w")
        self.card_number_entry = ttk.Entry(self.card_frame)
        self.card_number_entry.grid(row=0, column=1, sticky="ew", padx=(6,0))
        ttk.Label(self.card_frame, text="CVV:").grid(row=1, column=0, sticky="w", pady=(6,0))
        self.card_cvv_entry = ttk.Entry(self.card_frame, width=6, show="*")
        self.card_cvv_entry.grid(row=1, column=1, sticky="w", padx=(6,0), pady=(6,0))
        
        self.card_frame.grid_columnconfigure(1, weight=1)
        
        self.card_frame.pack(fill="x", padx=20, pady=5)

        self._toggle_payment_fields()

        # Back: restore parent window (OnlineOrderWindow) and close details
        tk.Button(self, text="Back", width=12, command=self._on_back,
                  bg="#eaf4ff", fg="#000000", activebackground="#d4ecff", activeforeground="#000000",
                  font=("Segoe UI", 11), relief="raised").pack(pady=6)

        tk.Button(self, text="Continue", width=25, command=self.submit,
                  bg="#eaf4ff", fg="#000000", activebackground="#d4ecff", activeforeground="#000000",
                  font=("Segoe UI", 11), relief="raised").pack(pady=10)

    def _on_back(self):
        try:
            if hasattr(self.master, 'deiconify'):
                    self.master.deiconify()
                    self.master.state("zoomed")
                    self.master.lift()
            self.destroy()
        except Exception:
                self.destroy()
           
        
    def toggle_address(self, event=None):
        if self.order_type.get() == "delivery":
            self.address_entry.configure(state="normal")
            self.compute_and_show_travel()
        else:
            self.address_entry.delete(0, "end")
            self.address_entry.configure(state="disabled")
            self.travel_label.config(text="")

    def _toggle_payment_fields(self):
        pm = self.payment_method.get()
        if pm == "card":
            self.card_frame.pack(fill="x", padx=20, pady=5)
            
        else:
            try:
                self.card_frame.forget()
            except Exception:
                self.card_frame.pack_forget()
                

    def submit(self):
        payment_method = self.payment_method.get()
        card_number = self.card_number_entry.get().strip() if payment_method == "card" else None
        card_cvv = self.card_cvv_entry.get().strip() if payment_method == "card" else None

        # If card selected, validate minimal card info (do not store these in DB)
        if payment_method == "card":
            if not card_number or not card_number.isdigit() or len(card_number) != 16:
                messagebox.showwarning("Invalid Card", "Please enter a valid 16-digit card number (digits only).")
                return
            if not card_cvv or not card_cvv.isdigit() or len(card_cvv) != 3:
                messagebox.showwarning("Invalid CVV", "Please enter a valid CVV (3 digits).")
                return
            card_last4 = card_number[-4:]
        else:
            card_last4 = None

        tip_amount = 0.0
        try:
            tip_txt = (self.tip_entry.get().strip() if getattr(self, 'tip_entry', None) else "")
            if tip_txt:
                tip_amount = float(tip_txt)
                if tip_amount < 0:
                    messagebox.showwarning("Invalid Tip", "Tip cannot be negative.")
                    return
            else:
                tip_amount = 0.0
        except Exception:
            messagebox.showwarning("Invalid Tip", "Please enter a numeric tip amount (e.g. 2.50).")
            return

        info = {
            "order_type": self.order_type.get(),
            "address": self.address_entry.get().strip(),
            "time": self.time_entry.get().strip(),
            "estimated_time": "",
            "comments": self.comments_entry.get().strip(),
            # indicate payment method and only reveal last 4 digits if card selected
            "payment_method": payment_method,
            "card_last4": card_last4,
            "tip_amount": tip_amount,
        }

        if info["order_type"] == "delivery" and not info["address"]:
            messagebox.showwarning("Missing address", "Address required for delivery.")
            return

        # Perform geocode once here and include result in info to avoid
        # a second network call (and to keep UI/debug consistency).
        if info["order_type"] == "delivery":
            coords, g_err = _geocode_debug(info["address"])
            info["coords"] = coords
            info["geocode_err"] = g_err
        else:
            info["coords"] = None
            info["geocode_err"] = None

        try:
            res = self.callback(info)
        except Exception as e:
            # callback raised — show error and keep this window open for retry
            try:
                messagebox.showerror("Order Error", f"An unexpected error occurred: {e}")
            except Exception:
                print("Order Error", e)
            return

        # Now handle the result returned by callback
        # True -> order placed successfully; close details
        # False -> user cancelled; close details and re-show parent
        # None -> DB error or failure; keep details open so user can retry
        try:
            if res is True:
                self.destroy()
                
                return

            if res is False:
                self.destroy()
                
                
                if hasattr(self.master, 'deiconify'):
                    self.master.deiconify()
                    self.master.state("zoomed")
                    
                    self.master.lift()
                        
                return           
            messagebox.showerror("Order Error", "Could not place order due to a database error. Please try again.")    
            return
        except Exception:
            return

    def compute_and_show_travel(self):
        addr = self.address_entry.get().strip()
        if not addr or self.order_type.get() != "delivery":
            self.travel_label.config(text="")
            return

        def worker():
            coords, err = _geocode_debug(addr)
            debug_msg = None
            estimated_time = None
            try:
                if not coords:
                    debug_msg = f"Geocode failed: {err} — using fallback 30 min"
                    total_min = 30
                else:
                    lat, lon = coords
                    rlat, rlon = RESTAURANT_COORDS
                    # Try OSRM routing for realistic duration
                    mins_osrm, osrm_dbg = _route_duration_osrm(rlat, rlon, lat, lon)
                    if mins_osrm is not None:
                        total_min = mins_osrm + 15
                        debug_msg = f"Geocoded: {lat:.5f},{lon:.5f} — {osrm_dbg} — total {total_min} min"
                    else:
                        # fallback to straight-line estimate at 25 km/h
                        dist_km = _haversine_km(rlat, rlon, lat, lon)
                        travel_min = (dist_km / 25.0) * 60.0
                        mins = max(0, int(round(travel_min)))
                        total_min = mins + 15
                        debug_msg = f"Geocoded: {lat:.5f},{lon:.5f} — dist {dist_km:.2f} km — travel {mins} min — total {total_min} min (OSRM err: {osrm_dbg})"
                # compute ETA time string
                try:
                    eta_dt = datetime.now() + timedelta(minutes=total_min)
                    estimated_time = eta_dt.strftime("%H:%M")
                except Exception:
                    estimated_time = None
            except Exception as e:
                debug_msg = f"Error computing travel: {e}"

            def ui_update():           
                if estimated_time:
                    self.travel_label.config(text=f"Estimated Time: {estimated_time}")
                else:
                    self.travel_label.config(text="Could not determine travel time")

            self.after(0, ui_update)
            

        t = threading.Thread(target=worker, daemon=True)
        t.start()


class OnlineOrderWindow(tk.Toplevel):
    def __init__(self, master, db, customer_data, welcome_back=True, return_callback=None):
        super().__init__(master)
        self.db = db
        self.customer_data = customer_data  # not yet saved
        self.welcome_back = bool(welcome_back)
        self.return_callback = return_callback

        self.title("Online Order")
        self.state("zoomed")
        self.bind("<Escape>", lambda e: self.state("normal"))
        self.resizable(True, True)

        ttk.Label(self, text="Select Items to Order",
                  font=("Arial", 14, "bold")).pack(pady=10)

        # Frame with scrollbar
        container = ttk.Frame(self)
        container.pack(fill="both", expand=True)

        canvas = tk.Canvas(container)
        canvas.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        scrollbar.pack(side="right", fill="y")

        canvas.configure(yscrollcommand=scrollbar.set)

        self.items_frame = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=self.items_frame, anchor="nw")

        self.items_frame.bind("<Configure>",
                              lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        self.load_items()

        tk.Button(self, text="Place Order", command=self.place_order, width=20,
              bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
              activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(pady=15)
        tk.Button(self, text="Back", command=self._on_back, width=12,
              bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
              activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(pady=6)


    def load_items(self):
        items = self.db.list_menu_items()

        self.item_widgets = []   # store item rows
        self.quantities = {}     # item_id -> Spinbox widget

        for item_id, desc, price, availability, allergens, tax_name in items:
            if not availability:
                continue

            row = ttk.Frame(self.items_frame)
            row.pack(fill="x", pady=5)

            ttk.Label(row, text=f"{desc} (${price:.2f})").pack(side="left", padx=10)

            qty = tk.Spinbox(row, from_=0, to=20, width=5)
            qty.pack(side="right", padx=10)

            self.quantities[item_id] = qty
            self.item_widgets.append(row)

    def place_order(self):
        items = []
        for item_id, widget in self.quantities.items():
            try:
                qty = int(widget.get())
            except:
                qty = 0

            if qty > 0:
                items.append((item_id, qty))

        if not items:
            messagebox.showwarning("Empty Order", "Please select at least one item.")
            return

        def open_details():
            self.withdraw()
           
            details_window = OnlineOrderDetailsWindow(self, callback=submit_details)

        def submit_details(info):
            order_type = info["order_type"]
            address = info["address"]
            time_txt = info["time"]
            comments = info["comments"]
            payment_method = info.get("payment_method", "card")
            # optional last4 (not sensitive) available in info but we don't store full card data
            card_last4 = info.get("card_last4")
            card_text = payment_method

            # Initialize geocode/routing info
            coords = info.get("coords") if isinstance(info, dict) else None
            g_err = info.get("geocode_err") if isinstance(info, dict) else None
            lat = None
            lon = None
            source = None
            dbg = None

            now_dt = datetime.now()
            if time_txt.strip():
                try:
                    requested_dt = datetime.strptime(time_txt, "%H:%M")
                    requested_dt = requested_dt.replace(year=now_dt.year, month=now_dt.month, day=now_dt.day)
                    diff_minutes = (requested_dt - now_dt).total_seconds() / 60

                    # Treat tiny negative differences (clock/seconds skew) as "now"
                    if -5 <= diff_minutes <= 30:
                        if order_type == "delivery":
                            # Use cached geocode from details window when available,
                            # otherwise perform geocode now.
                            if coords is None:
                                coords, g_err = _geocode_debug(address)
                            if coords:
                                lat, lon = coords
                                minutes_osrm, osrm_dbg = _route_duration_osrm(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                                if minutes_osrm is not None:
                                    travel_min = minutes_osrm
                                    source = "osrm"
                                    dbg = osrm_dbg
                                else:
                                    dist_km = _haversine_km(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                                    travel_min = max(5, int(round((dist_km / 35.0) * 60)))
                                    source = "haversine"
                                    dbg = f"geocoded {lat:.5f},{lon:.5f}; dist {dist_km:.2f} km; osrm_err: {osrm_dbg}"
                            else:
                                travel_min = 30
                                source = "geocode-failed"
                                dbg = g_err or "geocode failed"
                            total_min = travel_min + 15
                            estimated_time = (now_dt + timedelta(minutes=total_min)).strftime("%H:%M")
                        else:
                            estimated_time = (now_dt + timedelta(minutes=30)).strftime("%H:%M")
                    else:
                        estimated_time = time_txt
                except ValueError:
                    if order_type == "delivery":
                        if coords is None:
                            coords, g_err = _geocode_debug(address)
                        if coords:
                            lat, lon = coords
                            minutes_osrm, osrm_dbg = _route_duration_osrm(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                            if minutes_osrm is not None:
                                travel_min = minutes_osrm
                                source = "osrm"
                                dbg = osrm_dbg
                            else:
                                dist_km = _haversine_km(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                                travel_min = max(5, int(round((dist_km / 35.0) * 60)))
                                source = "haversine"
                                dbg = f"geocoded {lat:.5f},{lon:.5f}; dist {dist_km:.2f} km; osrm_err: {osrm_dbg}"
                        else:
                            travel_min = 30
                            source = "geocode-failed"
                            dbg = g_err or "geocode failed"
                        total_min = travel_min + 15
                        estimated_time = (now_dt + timedelta(minutes=total_min)).strftime("%H:%M")
                    else:
                        estimated_time = (now_dt + timedelta(minutes=30)).strftime("%H:%M")
            else:
                if order_type == "delivery":
                    if coords is None:
                        coords, g_err = _geocode_debug(address)
                    if coords:
                        lat, lon = coords
                        minutes_osrm, osrm_dbg = _route_duration_osrm(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                        if minutes_osrm is not None:
                            travel_min = minutes_osrm
                            source = "osrm"
                            dbg = osrm_dbg
                        else:
                            dist_km = _haversine_km(RESTAURANT_COORDS[0], RESTAURANT_COORDS[1], lat, lon)
                            travel_min = max(10, int(round((dist_km / 35.0) * 60)))
                            source = "haversine"
                            dbg = f"geocoded {lat:.5f},{lon:.5f}; dist {dist_km:.2f} km; osrm_err: {osrm_dbg}"
                    else:
                        travel_min = 30
                        source = "geocode-failed"
                        dbg = g_err or "geocode failed"
                    total_min = travel_min + 15
                    estimated_time = (now_dt + timedelta(minutes=total_min)).strftime("%H:%M")
                else:
                    estimated_time = (now_dt + timedelta(minutes=30)).strftime("%H:%M")

            # Defer opening DB write connection until after user confirms to reduce lock windows
            cur = None

            # Use a short-lived read connection to fetch menu item descriptions/prices
            read_conn = None
            read_cur = None
            try:
                read_conn = sqlite3.connect(DB_FILE, timeout=5)
                read_cur = read_conn.cursor()
            except Exception:
                read_conn = None
                read_cur = None

            total_cost = 0.0
            # If details window provided coords but lat/lon variables weren't
            # populated during ETA calculation, prefer the coords here
            if coords and (lat is None or lon is None):
                try:
                    lat, lon = coords
                except Exception:
                    lat = None
                    lon = None


            summary = "Your Order:\n\n"

            for item_id, qty in items:
                # Prefer the temporary read cursor for summary lookup; fall back to placeholder values
                if read_cur is not None:
                    try:
                        read_cur.execute("SELECT description, net_price FROM MENU_ITEM WHERE item_id=?", (item_id,))
                        row = read_cur.fetchone()
                        if row:
                            desc, price = row
                        else:
                            desc, price = (f"Item {item_id}", 0.0)
                    except Exception:
                        desc, price = (f"Item {item_id}", 0.0)
                else:
                    desc, price = (f"Item {item_id}", 0.0)

                subtotal = qty * price
                total_cost += subtotal
                summary += f"{desc} x {qty} = ${subtotal:.2f}\n"

            summary += f"\nTOTAL = ${total_cost:.2f}\n\n"
            summary += f"Order Type: {order_type}\n"
            if order_type == "delivery":
                summary += f"Address: {address}\n"
            summary += f"Requested Time: {time_txt}\n"
            summary += f"Estimated Time: {estimated_time}\n"
            summary += f"Comments: {comments}\n"

            if read_conn is not None:
                read_conn.close()
           
            if not messagebox.askyesno("Confirm Order", summary):
                return False

            # Ensure tip_amount is always defined 
            try:
                raw_tip = (info.get("tip_amount") if isinstance(info, dict) else None)
                if raw_tip is None or (isinstance(raw_tip, str) and raw_tip.strip() == ""):
                    tip_amount = 0.0
                else:
                    tip_amount = float(raw_tip)
            except Exception:
                tip_amount = 0.0

            conn = None
            try:
                conn = sqlite3.connect(DB_FILE, timeout=30)
                cur = conn.cursor()

                cur.execute("SELECT client_id FROM CLIENT WHERE phone=?", (self.customer_data["Phone"],))
                row = cur.fetchone()
                if row:
                    client_id = row[0]
                else:
                    cur.execute("""
                        INSERT INTO CLIENT (first_name, last_name, email, phone)
                        VALUES (?, ?, ?, ?)
                    """, (
                        self.customer_data["First Name"],
                        self.customer_data["Last Name"],
                        self.customer_data.get("Email"),
                        self.customer_data["Phone"]
                    ))
                    client_id = cur.lastrowid

                now_text = now_dt.strftime("%Y-%m-%d %H:%M")

                cur.execute("SELECT MAX(order_id) FROM ORDERS")
                row = cur.fetchone()
                order_id = 1 if row[0] is None else row[0] + 1

                cur.execute("INSERT INTO ORDERS (order_id, datetime) VALUES (?, ?)", (order_id, now_text))

                cur.execute(
                    """
                    INSERT INTO ONLINE_ORDER
                    (order_id, client_id, order_type, address, time, estimated_time, comments)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (order_id, client_id, order_type, address, time_txt, estimated_time, comments)
                )

                for item_id, qty in items:
                    cur.execute(
                        "INSERT INTO ORDER_CONSISTS_OF_MENU_ITEM (order_id, item_id, quantity) VALUES (?, ?, ?)",
                        (order_id, item_id, qty)
                    )

                try:
                    cur.execute("SELECT MAX(receipt_no) FROM RECEIPT")
                    r = cur.fetchone()
                    next_receipt_no = 9001 if not r or r[0] is None else int(r[0]) + 1
                except Exception:
                    next_receipt_no = None

                now_time = datetime.now().strftime("%H:%M")
                today = datetime.now().strftime("%Y-%m-%d")

                if next_receipt_no is not None:
                    try:
                        raw_tip = (info.get("tip_amount") if isinstance(info, dict) else None)
                        if raw_tip is None or (isinstance(raw_tip, str) and raw_tip.strip() == ""):
                            tip_amount = 0.0
                        else:
                            tip_amount = float(raw_tip)
                    except Exception:
                        tip_amount = 0.0
                    for item_id, qty in items:
                        try:
                            cur.execute("SELECT net_price FROM MENU_ITEM WHERE item_id = ?", (item_id,))
                            row = cur.fetchone()
                            price = float(row[0]) if row and row[0] is not None else 0.0
                        except Exception:
                            price = 0.0
                        subtotal = qty * price
                        
                            # For online/card orders mark receipts as paid_off=1
                        cur.execute(
                                "INSERT INTO RECEIPT (item_id, qty, tips, total_amount, receipt_no, order_id, paid_off, time, date, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (item_id, qty, tip_amount, subtotal, next_receipt_no, order_id, 1, now_time, today, card_text),
                            )

                conn.commit()
                
                items_payload = []
                try:
                    for i, q in items:
                        desc = None
                        try:
                            cur.execute("SELECT description FROM MENU_ITEM WHERE item_id = ?", (i,))
                            r = cur.fetchone()
                            if r and r[0] is not None:
                                desc = r[0]
                        except Exception:
                            desc = None
                        items_payload.append({"item_id": int(i), "qty": int(q), "description": desc})
                except Exception:
                    items_payload = [{"item_id": int(i), "qty": int(q)} for i, q in items]

                payload = {
                    "order_id": order_id,
                    "kind": "online",
                    "order_type": order_type,
                    "client": {
                        "first_name": self.customer_data.get("First Name") if getattr(self, 'customer_data', None) else None,
                        "last_name": self.customer_data.get("Last Name") if getattr(self, 'customer_data', None) else None,
                        "phone": self.customer_data.get("Phone") if getattr(self, 'customer_data', None) else None,
                        "email": self.customer_data.get("Email") if getattr(self, 'customer_data', None) else None,
                    },
                    "address": address,
                    "estimated_time": estimated_time,
                    "items": items_payload,
                    "payment_method": card_text,
                    "tip_amount": tip_amount,
                    "datetime": now_text,
                }
                
                publish_order_event(payload)
                 
            except Exception as e:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                messagebox.showerror("Order Error", f"Could not place order: {e}")
                # return None to indicate an error so the details dialog can reopen
                return None
            finally:
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass

            messagebox.showinfo(
                "Order Placed",
                f"Order #{order_id} has been placed successfully!\nEstimated Time: {estimated_time}"
            )

            self.show_receipt_window(
                order_items=items,
                client_address=address if order_type == "delivery" else "TAKEAWAY",
                payment_method=payment_method,
                order_datetime=now_dt
            )
            return True

        open_details()

    def show_receipt_window(self, order_items, client_address, payment_method, order_datetime):
        receipt = tk.Toplevel(self)
        receipt.title("Receipt")
        receipt.geometry("360x550")
        receipt.resizable(False, False)

        mono = ("Courier New", 10)

        def line(text=""):
            ttk.Label(receipt, text=text, font=mono).pack(anchor="w", padx=15)

        # HEADER
        ttk.Label(receipt, text="Veni, vidi, edi", font=("Courier New", 14, "bold")).pack(pady=(10, 2))
        line("123 Food Street")
        line("Tel: 210-1234567")
        line("--------------------------------")
        line(order_datetime.strftime("%d/%m/%Y %H:%M"))
        line("--------------------------------")

        if client_address and client_address != "TAKEAWAY":
            line("DELIVERY TO:")
            line(client_address)
            line("--------------------------------")
        else:
            line("TAKEAWAY")
            line("--------------------------------")

        total = 0.0

        # ITEMS
        for item_id, qty in order_items:
            cur = self.db._get_conn().cursor()
            cur.execute(
                "SELECT description, net_price FROM MENU_ITEM WHERE item_id = ?",
                (item_id,)
            )
            row = cur.fetchone()
            if not row:
                continue

            desc, price = row
            subtotal = qty * price
            total += subtotal

            line(f"{desc}")
            line(f" {qty} x {price:.2f}€      {subtotal:.2f}€")

        line("--------------------------------")
        line(f"TOTAL:              {total:.2f}€")
        line("--------------------------------")

        pay = "CARD" if payment_method == "card" else "CASH"
        line(f"PAYMENT: {pay}")
        line("")
        line("Thank you for your order!")
        line("")

         # ΟΤΑΝ ΚΛΕΙΣΕΙ Η ΑΠΟΔΕΙΞΗ
        def on_close():
            try:
                self.destroy()   # κλείνει το OnlineOrderWindow
            except:
                pass
            try:
                parent_root = getattr(self, 'master', None)
                if parent_root:
                    parent_root.destroy()
            except:
                
                pass

            login_app = LoginApp(self.db)
            login_app.mainloop()

        receipt.protocol("WM_DELETE_WINDOW", on_close)
        ttk.Button(receipt, text="Close", command=on_close).pack(pady=10)

    def _on_back(self):
        
        if callable(getattr(self, "return_callback", None)):
            self.destroy()
            self.return_callback()
            return    
        self.destroy()
   
class CustomerInfoWindow(tk.Toplevel):
    def __init__(self, master, db, mode="order", return_callback=None):
        super().__init__(master)
        self.db = db
        self.mode = mode  # "order" or "reservation"
        self.return_callback = return_callback

        self.title("Customer Information")
        self.state("zoomed")
        self.bind("<Escape>", lambda e: self.state("normal"))

        self.minsize(1024, 768)
        self.resizable(True, True)

        ttk.Label(self, text="Enter Your Information", font=("Arial", 14, "bold")).pack(pady=10)

        self.entries = {}

        for field in ["First Name", "Last Name", "Phone"]:
            ttk.Label(self, text=field + ":").pack(anchor="w", padx=20)
            entry = ttk.Entry(self)
            entry.pack(fill="x", padx=20, pady=5)
            self.entries[field] = entry

        if self.mode == "order":
            ttk.Label(self, text="Email:").pack(anchor="w", padx=20)
            entry = ttk.Entry(self)
            entry.pack(fill="x", padx=20, pady=5)
            self.entries["Email"] = entry

        tk.Button(self, text="Continue", width=25, command=self.submit,
                  bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
                  activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(pady=10)
        if callable(self.return_callback):
            tk.Button(self, text="Back", command=self._on_back, width=12,
                      bg="#eaf4ff", fg="#000000", activebackground="#d4ecff",
                      activeforeground="#000000", font=("Segoe UI", 11), relief="raised").pack(pady=6)

    def submit(self):
        data = {k: v.get().strip() for k, v in self.entries.items()}

        required_fields = ["First Name", "Last Name", "Phone"]
        if self.mode == "order":
            required_fields.append("Email")

        for field in required_fields:
            if not data.get(field):
                messagebox.showwarning("Missing Info", f"{field} is required!")
                return

        phone = data.get("Phone", "")
        if not re.fullmatch(r"69\d{8}", phone):
            messagebox.showwarning("Invalid Phone", "Please enter a valid phone number.")
            return

        if self.mode == "order":
            email = data.get("Email", "")
            email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
            if not re.fullmatch(email_pattern, email):
                messagebox.showwarning("Invalid Email", "Please enter a valid email address.")
                return

        # keep the form around so OnlineOrderWindow can return to it
        if self.mode == "order":
            # If the phone already exists in DB, greet the client 
            conn = self.db._get_conn()
            cur = conn.cursor()
            try:
                cur.execute("SELECT first_name, last_name, client_id FROM CLIENT WHERE phone = ?", (phone,))
                r = cur.fetchone()
                if r:
                    name = (r[0] or "").strip()
                    if not name:
                        name = (r[1] or "").strip()
                    client_id = r[2]
                    # Build welcome text including up to 5 recent orders
                    welcome_lines = []
                    if name:
                        welcome_lines.append(f"Welcome back, {name}!")
                    else:
                        welcome_lines.append("Welcome back!")

                    try:
                        cur.execute(
                            """
                            SELECT o.order_id, o.datetime
                            FROM ORDERS o
                            JOIN ONLINE_ORDER io ON io.order_id = o.order_id
                            WHERE io.client_id = ?
                            ORDER BY o.datetime DESC
                            LIMIT 5
                            """,
                            (client_id,)
                        )
                        recent = cur.fetchall() or []
                    except Exception:
                        recent = []

                    if recent:
                        welcome_lines.append("")
                        welcome_lines.append("Recent Orders:")
                        for ord_row in recent:
                            try:
                                oid = ord_row[0]
                                dt = ord_row[1]
                                # fetch items for this order
                                items_txt = []
                                try:
                                    cur.execute(
                                        "SELECT oc.item_id, oc.quantity, m.description FROM ORDER_CONSISTS_OF_MENU_ITEM oc LEFT JOIN MENU_ITEM m ON m.item_id = oc.item_id WHERE oc.order_id = ?",
                                        (oid,)
                                    )
                                    its = cur.fetchall() or []
                                    for it in its:
                                        iid = it[0]
                                        qty = it[1]
                                        desc = it[2] or f"Item {iid}"
                                        items_txt.append(f"{desc} x{qty}")
                                except Exception:
                                    items_txt = []

                                # compute total from RECEIPT if available
                                total = None
                                try:
                                    cur.execute("SELECT COALESCE(SUM(total_amount + COALESCE(tips,0)),0) FROM RECEIPT WHERE order_id = ?", (oid,))
                                    trow = cur.fetchone()
                                    if trow and trow[0] is not None:
                                        total = float(trow[0])
                                except Exception:
                                    total = None

                                line = f"#{oid} @ {dt}"
                                if total is not None:
                                    line += f" — Total: ${total:.2f}"
                                welcome_lines.append(line)
                                if items_txt:
                                    welcome_lines.append("  " + "; ".join(items_txt))
                            except Exception:
                                continue
                    # show the constructed welcome message
                    try:
                        messagebox.showinfo("Welcome Back", "\n".join(welcome_lines))
                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
         
          
            self.withdraw()
            OnlineOrderWindow(self.master, self.db, customer_data=data, return_callback=self._on_return)
        else:
            self.destroy()
            
            messagebox.showinfo("Reservation", "Customer info saved! Proceed to make a reservation.")

    def _on_return(self):
        
            self.deiconify()    
            self.state("zoomed")
            
            self.lift()
        
    def _on_back(self):
        
        if callable(self.return_callback):    
            self.destroy()
            self.return_callback()
                
            return
        self.destroy()
        
def main():
    db = Database(DB_FILE)
    app = LoginApp(db)
    app.mainloop()

if __name__ == "__main__":
    main()

