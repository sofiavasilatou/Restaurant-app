import json
import queue
import threading
import os
import sys
import time
from datetime import datetime, timezone

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
except Exception:
    print("Tkinter is required to run this UI.")
    raise

try:
    import paho.mqtt.client as mqtt
except Exception:
    print("Please install paho-mqtt: pip install paho-mqtt")
    raise

# Configuration
BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
BRANCH = os.environ.get("RESTAURANT_BRANCH", "mybranch")
TOPIC = f"restaurant/{BRANCH}/orders/new"
READY_TOPIC = f"restaurant/{BRANCH}/orders/ready"
MQTT_CLIENT = None
MQTT_QOS = 1

# Thread-safe queue for handoff from mqtt thread to Tk main thread
_msg_queue = queue.Queue()

# Simple in-memory store (order_id -> payload) for display
ORDERS = {}


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker {BROKER_HOST}:{BROKER_PORT}")
        client.subscribe(TOPIC, qos=MQTT_QOS)
    else:
        print("MQTT connect failed rc=", rc)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
    except Exception as e:
        print("Failed to decode message", e)
        return
    # push to UI queue
    _msg_queue.put(data)


def start_mqtt_loop():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Optionally set username/password via env
    user = os.environ.get("MQTT_USER")
    pwd = os.environ.get("MQTT_PASS")
    if user:
        client.username_pw_set(user, pwd)


    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    except Exception as e:
        print("Could not connect to MQTT broker:", e)
        return None

    client.loop_start()
    try:
        # expose client for publishing ready events
        globals()['MQTT_CLIENT'] = client
    except Exception:
        pass
    return client


class KitchenUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Kitchen - Live Orders")
        self.geometry("700x500")

        toolbar = ttk.Frame(self, padding=6)
        toolbar.pack(fill="x")

        ttk.Label(toolbar, text=f"Broker: {BROKER_HOST}:{BROKER_PORT}").pack(side="left")
        ttk.Label(toolbar, text=f"Topic: {TOPIC}").pack(side="left", padx=(12,0))
        ttk.Button(toolbar, text="Mark Ready", command=self.mark_selected_ready).pack(side="right", padx=(6,0))
        ttk.Button(toolbar, text="Clear", command=self.clear_list).pack(side="right")

        # Use a vertical paned window so the list and details panes can be
        # resized by the user and the details area can be larger.
        paned = tk.PanedWindow(self, orient='vertical')
        paned.pack(fill='both', expand=True, padx=8, pady=8)

        # Top pane: list of orders (newest first)
        top_frame = ttk.Frame(paned)
        self.listbox = tk.Listbox(top_frame, height=12, font=(None, 11))
        self.listbox.pack(fill="both", expand=True)
        self.listbox.bind("<<ListboxSelect>>", self.on_select)
        paned.add(top_frame)

        # Bottom pane: details area (larger by default and resizable)
        bottom_frame = ttk.Frame(paned)
        self.details = tk.Text(bottom_frame, height=14, state='disabled', wrap='word')
        self.details.pack(fill="both", expand=True)
        paned.add(bottom_frame)

        # Poll the message queue
        self.after(200, self.poll_queue)

    def clear_list(self):
        global ORDERS
        ORDERS.clear()
        self.listbox.delete(0, 'end')
        self.details.configure(state='normal')
        self.details.delete('1.0', 'end')
        self.details.configure(state='disabled')

    def poll_queue(self):
        updated = False
        try:
            while True:
                data = _msg_queue.get_nowait()
                self.handle_incoming_order(data)
                updated = True
        except queue.Empty:
            pass
        # schedule next poll
        self.after(200, self.poll_queue)

    def handle_incoming_order(self, data):
        # Expect data to include order_id
        order_id = data.get('order_id') or data.get('id') or int(time.time())
        ORDERS[order_id] = data
        # Insert at top of listbox
        text = self._order_summary_text(data)
        try:
            self.listbox.insert(0, text)
        except Exception:
            pass

    def mark_selected_ready(self):
        # Determine selected order id from selection or top item
        sel = self.listbox.curselection()
        if sel:
            idx = sel[0]
            text = self.listbox.get(idx)
        else:
            try:
                text = self.listbox.get(0)
            except Exception:
                return
        try:
            oid = int(text.split()[0].lstrip('#'))
        except Exception:
            return

        # publish ready event (include table number when available)
        payload = {"order_id": int(oid), "ready": True, "timestamp": datetime.now(timezone.utc).isoformat()}
        try:
            record = ORDERS.get(int(oid))
            if record:
                # include table_no if present in the original order payload
                t = record.get('table_no') if isinstance(record, dict) else None
                if t is not None:
                    payload['table_no'] = t
        except Exception:
            pass
        # try to use shared MQTT client if available
        try:
            client = globals().get('MQTT_CLIENT')
            if client:
                try:
                    info = client.publish(READY_TOPIC, json.dumps(payload, ensure_ascii=False), qos=1)
                    try:
                        info.wait_for_publish(timeout=3)
                    except Exception:
                        pass
                except Exception:
                    pass
            else:
                # fallback: create one-off client
                try:
                    tmp = mqtt.Client()
                    tmp.connect(BROKER_HOST, BROKER_PORT, 60)
                    tmp.loop_start()
                    info = tmp.publish(READY_TOPIC, json.dumps(payload, ensure_ascii=False), qos=1)
                    try:
                        info.wait_for_publish(timeout=3)
                    except Exception:
                        pass
                    tmp.disconnect()
                    tmp.loop_stop()
                except Exception:
                    pass
        except Exception:
            pass

        # mark as ready in UI
        try:
            new_text = f"[READY] {text}"
            if sel:
                self.listbox.delete(idx)
                self.listbox.insert(idx, new_text)
            else:
                # replace top item
                self.listbox.delete(0)
                self.listbox.insert(0, new_text)
        except Exception:
            pass

    def _order_summary_text(self, data):
        oid = data.get('order_id', '?')
        kind = data.get('kind', 'unknown')
        table = data.get('table_no')
        # Prefer explicit datetime fields, fall back to timestamp or now
        ts_raw = data.get('datetime') or data.get('timestamp') or datetime.now(timezone.utc).isoformat()
        try:
            # Normalize ISO timestamps to local readable form without microseconds
            try:
                ts_dt = datetime.fromisoformat(ts_raw)
            except Exception:
                ts_dt = datetime.fromisoformat(ts_raw.replace('Z', '+00:00')) if isinstance(ts_raw, str) else datetime.now(timezone.utc)
            ts = ts_dt.replace(microsecond=0).isoformat(sep=' ')
        except Exception:
            ts = str(ts_raw)
        items = data.get('items') or []
        parts = [f"#{oid}", f"{kind}"]
        if table is not None:
            parts.append(f"Table:{table}")
        parts.append(f"Items:{len(items)}")
        parts.append(f"@ {ts}")
        return " ".join(parts)

    def on_select(self, evt):
        sel = self.listbox.curselection()
        if not sel:
            return
        # Listbox is newest-first; index 0 is newest
        idx = sel[0]
        text = self.listbox.get(idx)
        # try to extract order_id from text (after '#')
        try:
            oid = int(text.split()[0].lstrip('#'))
        except Exception:
            # fallback: show raw
            self._show_details({'raw': text})
            return
        data = ORDERS.get(oid)
        if not data:
            self._show_details({'raw': text})
            return
        self._show_details(data)

    def _show_details(self, data):
        # Render a friendly details view instead of raw JSON
        lines = []
        def add(k, v):
            lines.append(f"{k}: {v}")

        add("Order ID", data.get('order_id', 'N/A'))
        add("Kind", data.get('kind', 'N/A'))
        if data.get('order_type'):
            add("Order Type", data.get('order_type'))
        if data.get('table_no') is not None:
            add("Table", data.get('table_no'))
        # client info, if present
        client = data.get('client') or {}
        if client:
            add("Client", f"{client.get('first_name','') } {client.get('last_name','') } ({client.get('phone','')})")
            if client.get('email'):
                add("Email", client.get('email'))
        if data.get('address'):
            add("Address", data.get('address'))
        if data.get('estimated_time'):
            add("ETA", data.get('estimated_time'))
        if data.get('payment_method'):
            add("Payment Method", data.get('payment_method'))
        if data.get('tip_amount') is not None:
            add("Tip", data.get('tip_amount'))

        # Items list
        items = data.get('items') or []
        lines.append("")
        lines.append("Items:")
        if items:
            for it in items:
                try:
                    iid = it.get('item_id', it.get('id', '?'))
                    qty = it.get('qty', it.get('quantity', 1))
                    # if a description is provided include it
                    desc = it.get('description') or it.get('desc')
                    if desc:
                        lines.append(f" - {desc} (#{iid}) x{qty}")
                    else:
                        lines.append(f" - Item #{iid} x{qty}")
                except Exception:
                    lines.append(f" - {json.dumps(it)}")
        else:
            lines.append(" (no items)")

        # Timestamp
        ts = data.get('datetime') or data.get('timestamp')
        if ts:
            lines.append("")
            lines.append(f"Placed: {ts}")

        self.details.configure(state='normal')
        self.details.delete('1.0', 'end')
        self.details.insert('1.0', '\n'.join(lines))
        self.details.configure(state='disabled')


def main():
    client = start_mqtt_loop()
    if client is None:
        print("MQTT client failed to start. Exiting.")
        sys.exit(1)

    app = KitchenUI()
    try:
        app.mainloop()
    finally:
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass


if __name__ == '__main__':
    main()

