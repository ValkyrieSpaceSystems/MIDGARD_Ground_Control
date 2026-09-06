import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv, tomllib, importlib.util, asyncio, json, sqlite3, uvicorn, subprocess, urllib.request, urllib.error
import numpy as np
from queue import PriorityQueue, Queue, Empty, Full
from datetime import datetime, UTC

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, LineGrouping, WAIT_INFINITELY
import nidaqmx.system
from labjack import ljm
import Basilisk

from midgard_functions import gv, print_out, error_out, get_element, combine_with_and, label, run, check_and_install_openmct, check_configs, write_actuation, abort, unabort, shutdown


def _safe_key(*parts):
    """Join parts into a stable, collision-safe identifier key."""
    return ".".join(re.sub(r"[^a-zA-Z0-9_-]", "_", str(p)) for p in parts)

def buildOpenMCTjs(output_path=None): # CANNOT USE print_out
    output_path = output_path or os.path.join(
        os.path.dirname(__file__), "openmct_midgard", "telemetry-tree.js"
    )

    data_tree = {}
    all_keys = []
    peripheral_folders = [ # Hard coded MIDGARD level abort, logging, and terminal (only terminal active because the others can be easily tied to config files and are hard to connect to sync groups)
        #{"name": 'Abort', "key": _safe_key('abort'), "control": True, "measurement": {'format': 'enum', 'enumerations': [{'value': 0, 'string': 'Safe'}, {'value': 1, 'string': 'Aborted'}]}},
        #{"name": 'Logging', "key": _safe_key('logging'), "control": True, "measurement": {'format': 'enum', 'enumerations': [{'value': 0, 'string': 'Off'}, {'value': 1, 'string': 'On'}]}},
        {"name": 'Terminal', "key": _safe_key('terminal_log'), "measurement": {'units': 'str', 'format': 'string'}},
        ]

    for peripheral_name, peripheral in gv.peripherals.items():
        data_tree[peripheral_name] = {}
        element_class_folders = []

        for element_class, element_dict in peripheral.element_lookup.items():
            if not element_dict:
                continue
            elif element_class in ['Interface', 'NI_Module']:
                continue

            data_tree[peripheral_name][element_class] = {}

            # group leaves by (type, subtype) — either or both may be None
            groups = {}
            for element_id, element in element_dict.items():
                data_tree[peripheral_name][element_class][element_id] = element

                element_type = getattr(element, 'type', None)
                element_subtype = getattr(element, 'subtype', None)

                key = _safe_key(peripheral_name, element_class, element_id)
                element.key = key
                all_keys.append(key)

                control_type = getattr(element, 'control_type', None)

                leaf = {}
                if control_type == 'button':
                    leaf = {"name": element.name, "key": key, "trigger": True}

                elif control_type in ('switch', 'selector'):
                    if control_type == 'switch':
                        enumerations = [{'value': 0, 'string': element.nominal_state}, {'value': 1, 'string': element.off_nominal_state}]
                    else:
                        enumerations = [{'value': i, 'string': str(s)} for i, s in enumerate(element.states)]

                    meas_format = {'format': 'enum', 'enumerations': enumerations}

                    selected_color = getattr(element, 'selected_color', None)
                    unselected_color = getattr(element, 'unselected_color', None)
                    if selected_color or unselected_color:
                        meas_format['style'] = {'selected': selected_color, 'unselected': unselected_color}

                    leaf = {"name": element.name, "key": key, "control": True, "measurement": meas_format}

                    switch_display = {}
                    switch_display.update(getattr(peripheral, 'switch_display', {}) or {})
                    switch_display.update(getattr(element, 'switch_display', {}) or {})
                    if switch_display:
                        leaf["switch_display"] = switch_display
                        
                elif control_type == 'message':
                    leaf = {"name": element.name, "key": key, "measurement": {'units': 'str', 'format': 'string'}}

                elif element_class == 'Data_Stream':
                    # DataStream, or an Actuator with no control_type (servo)
                    unit = getattr(element, 'unit', None)
                    if unit == 'bool':
                        meas_format = {'format': 'enum', 'enumerations': [{'value': False, 'string': 'False'}, {'value': True, 'string': 'True'}]}
                    elif unit == 'str':
                        meas_format = {'units': unit, 'format': 'string'}
                    else:
                        meas_format = {'units': unit, 'format': 'number'}
                    leaf = {"name": element.name, "key": key, "control": False, "measurement": meas_format}
                        
                if leaf:
                    groups.setdefault((element_type, element_subtype), []).append(leaf)

            # fold groups into a folder tree: [type folder ->] [subtype folder ->] leaves
            type_buckets = {}   # type_or_None -> {"leaves": [...], "subtypes": {subtype: [...]}}
            for (element_type, element_subtype), leaves in groups.items():
                bucket = type_buckets.setdefault(element_type, {"leaves": [], "subtypes": {}})
                if element_subtype:
                    bucket["subtypes"].setdefault(element_subtype, []).extend(leaves)
                else:
                    bucket["leaves"].extend(leaves)

            element_folders = []
            for element_type, bucket in type_buckets.items():
                if element_type is None:
                    # no type at all — sits directly in the element_class folder
                    element_folders.extend(bucket["leaves"])
                    for subtype_name, sub_leaves in bucket["subtypes"].items():
                        element_folders.append({
                            "name": label(subtype_name),
                            "key": _safe_key(peripheral_name, element_class, "untyped", subtype_name),
                            "children": sub_leaves,
                        })
                    continue

                type_folders = list(bucket["leaves"])
                for subtype_name, sub_leaves in bucket["subtypes"].items():
                    type_folders.append({
                        "name": label(subtype_name),
                        "key": _safe_key(peripheral_name, element_class, element_type, subtype_name),
                        "children": sub_leaves,
                    })

                element_folders.append({
                    "name": label(element_type),
                    "key": _safe_key(peripheral_name, element_class, element_type),
                    "children": type_folders,
                })

            if element_folders:
                element_class_folders.append({
                    "name": element_class,
                    "key": _safe_key(peripheral_name, element_class),
                    "children": element_folders,
                })

        peripheral_folders.append({
            "name": peripheral.display_name,
            "key": _safe_key(peripheral_name),
            "children": element_class_folders,
        })

    tree = {"name": "MIDGARD", "key": "midgard_root", "children": peripheral_folders}
    js = "// Auto-generated by buildOpenMCTjs() — do not edit by hand.\n"
    js += "var MIDGARD_TELEMETRY_PORT = " + str(gv.telemetry_port) + ";\n"
    js += "var TELEMETRY_TREE = " + json.dumps(tree, indent=4) + ";\n"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(js)

    #print(data_tree, all_keys)
    gv.data_tree = data_tree 
    gv.all_keys = all_keys


class OpenMCTServer:
    def __init__(self, static_dir=os.path.join(os.path.dirname(__file__), "openmct_midgard")):
        self.port = gv.openmct_port
        self.show_logs = gv.show_server_logs
        self.dist_dir = os.path.join(gv.openmct_dir, "dist")
        self.static_dir = static_dir  # your index.html, dictionary.js, plugin files

        if not os.path.isdir(self.dist_dir):
            raise FileNotFoundError(
                f"{self.dist_dir} not found — run `npm run build` inside {gv.openmct_dir} first and make sure node.js and npm are installed properly"
            )

        self.app = FastAPI()
        if self.show_logs:
            @self.app.middleware("http")
            async def _log_requests(request, call_next, _tag=self.__class__.__name__):
                response = await call_next(request)
                print(f"[{_tag}] {request.method} {request.url.path} -> {response.status_code}")
                return response
        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
        )
        self.app.mount("/openmct", StaticFiles(directory=self.dist_dir), name="openmct")
        self.app.mount("/", StaticFiles(directory=self.static_dir, html=True), name="openmct_midgard")

        self._server = None
        self._thread = None

    def start(self):
        if self.is_running:
            print("[OpenMCTServer] is already running.")
            return
                            
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.port,
            log_config=None,     # stop uvicorn from touching the shared/global logging config
            access_log=False,    # suppress its built-in per-request access log
        )
        self._server = uvicorn.Server(config)

        def _run():
            self._server.run()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

        while not getattr(self._server, "started", False):
            time.sleep(0.01)

        print(f"[OpenMCTServer] started on port {self.port}")

    def stop(self, delete_db=False):
        if not self.is_running:
            print("[OpenMCTServer] is not running.")
            return

        self._server.should_exit = True
        self._thread.join(timeout=5)
        self._server = None
        self._thread = None
        print("[OpenMCTServer] stopped.")

    def restart(self):
        self.stop()
        self.start()

    @property
    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    
class TelemetryServer:
    def __init__(self, db_path=None, buffer_interval=0.3):
        self.stop_event = gv.stop_event
        self.logging = False
        self.log_data = gv.log_data
        self.log_actuations = gv.log_actuations
        self.log_output_dir = gv.log_output_dir if gv.log_output_dir != '' else None
        self.port = gv.telemetry_port
        self.db_path = db_path or os.path.join(os.path.dirname(__file__), "telemetry.db")
        self.show_logs = gv.show_server_logs
        self.buffer_interval = buffer_interval   # seconds; None = always write immediately
        self._db_lock = threading.Lock()
        self._db_conn = None
        self._csv_lock = threading.Lock()
        self.csv_path = None
        self.csv_file = None
        self.csv_files = []
        self._write_buffer = []
        self._log_buffer = []
        self._buffer_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._flush_thread = None
        self._flush_stop = None
        self._flush_log_thread = None
        self._flush_log_stop = None

        self.app = FastAPI()
        if self.show_logs:
            @self.app.middleware("http")
            async def _log_requests(request, call_next, _tag=self.__class__.__name__):
                response = await call_next(request)
                print(f"[{_tag}] {request.method} {request.url.path} -> {response.status_code}")
                return response
        self.app.add_middleware(
            CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
        )

        self._clients = []      # connected WebSocket objects
        self._loop = None       # asyncio loop running inside the server thread
        self._server = None     # uvicorn.Server instance, used to request shutdown
        self._thread = None
        self.command_queue = Queue()

        self._init_db()
        self._register_routes()

    # ---------------- storage ----------------

    def _init_db(self):
        self._db_conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        self._db_conn.execute("PRAGMA journal_mode=WAL")
        self._db_conn.execute("PRAGMA busy_timeout=5000")
        with self._db_lock:
            self._db_conn.execute(
                "CREATE TABLE IF NOT EXISTS telemetry (key TEXT NOT NULL, value REAL, utc INTEGER NOT NULL, source TEXT, inhibited TEXT)"
            )
            self._db_conn.execute("CREATE INDEX IF NOT EXISTS idx_key_utc ON telemetry(key, utc)")
            self._db_conn.commit()

    def _store(self, key, value, utc, source=None, inhibited=None):
        with self._db_lock:
            self._db_conn.execute(
                "INSERT INTO telemetry (key, value, utc, source, inhibited) VALUES (?, ?, ?, ?, ?)", (key, value, utc, source, json.dumps(inhibited))
            )
            self._db_conn.commit()

    def _query_history(self, key, start, end):
        with self._db_lock:
            rows = self._db_conn.execute(
                "SELECT value, utc, source, inhibited FROM telemetry WHERE key = ? AND utc BETWEEN ? AND ? ORDER BY utc",
                (key, start, end),
            ).fetchall()
        return [{"key": key, "value": v, "utc": u, "source": s, "inhibited": json.loads(i)} for v, u, s, i in rows]
    
    def _flush_buffer(self):
        with self._buffer_lock:
            if not self._write_buffer:
                return
            batch, self._write_buffer = self._write_buffer, []
        with self._db_lock:
            self._db_conn.executemany(
                "INSERT INTO telemetry (key, value, utc, source, inhibited) VALUES (?, ?, ?, ?, ?)", batch
            )
            self._db_conn.commit()

    def _flush_loop(self):
        while not self._flush_stop.is_set():
            self._flush_stop.wait(self.buffer_interval)
            self._flush_buffer()
        self._flush_buffer()   # final flush so nothing pending is lost on stop

    # ---------------- routes ----------------

    def _register_routes(self):
        app = self.app

        @app.get("/history/{key}")
        def get_history(key: str, start: int = 0, end: int = None):
            end = end if end is not None else int(time.time() * 1000)
            return self._query_history(key, start, end)

        @app.websocket("/realtime")
        async def realtime(websocket: WebSocket):
            await websocket.accept()
            self._clients.append(websocket)
            if self.show_logs:
                print(f"[TelemetryServer] client connected ({len(self._clients)} clients)")
            try:
                while True:
                    raw = await websocket.receive_text()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("cmd") == "request" and "key" in msg and "requested" in msg:
                        self.command_queue.put_nowait({"key": msg["key"], "requested": msg["requested"], "pressed_keys": msg.get("pressed_keys", [])})
            except WebSocketDisconnect:
                pass
            finally:
                if websocket in self._clients:
                    self._clients.remove(websocket)
                if self.show_logs:
                    print(f"[TelemetryServer] client disconnected ({len(self._clients)} clients)")

    # ---------------- logging ----------------
    
    def _flush_log_buffer(self):
        with self._log_lock:
            if not self._log_buffer:
                return
            batch, self._log_buffer = self._log_buffer, []
        with self._csv_lock:
            self.csv_writer.writerows(batch)

    def _flush_log_loop(self):
        while not self._flush_log_stop.is_set():
            self._flush_log_stop.wait(self.buffer_interval)
            self._flush_log_buffer()
        self._flush_log_buffer() 
    
    def start_logging(self):
        try:
            if not self.logging:
                self.logging = True
                self.csv_path = os.path.join(self.log_output_dir,f"{time.strftime("%Y-%m-%d_%H-%M-%S")}.csv") if self.log_output_dir is not None else os.path.join(os.path.dirname(__file__), "logs", f"{time.strftime("%Y-%m-%d_%H-%M-%S")}.csv")
                self.csv_files.append(self.csv_path)
                
                header = ["unix_time_ns", "key", "value", "source", "inhibited"]
                self.csv_file = open(f'{self.csv_path}', 'a', newline='', buffering=1<<16)
                self.csv_writer = csv.writer(self.csv_file)
                self.csv_writer.writerow(header)
        
                if self.buffer_interval:
                    self._flush_log_stop = threading.Event()
                    self._flush_log_thread = threading.Thread(target=self._flush_log_loop, daemon=True)
                    self._flush_log_thread.start()
        
                print(f"[Logging] File Started: {self.csv_path}")
                
            else:
                print('[Logging] already running')
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[Logging] Start Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    
    def stop_logging(self):
        try:
            if self.logging:
                self.logging = False
                
                if self._flush_thread:
                    self._flush_log_stop.set()
                    self._flush_log_thread.join(timeout=2)
                    self._flush_log_thread = None
                
                with self._log_lock:
                    self.csv_file.close()
                    self._csv_file = None
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[Logging] Stop Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

    # ---------------- publish data ----------------

    def send(self, utc: int, key: str, value, element, source: str, inhibited=False, immediate=False, push_to_gui=True):
        try:
            if not self.is_running:
                print('[TelemetryServer] Cannot send data because server is not running')
            
            if isinstance(value, bool):
                value = int(value)
            
            def _push(utc, key, value, source, push_to_gui, inhibited=None):
                try:
                    if self.is_running:
                        if self.buffer_interval and not immediate:
                            with self._buffer_lock:
                                self._write_buffer.append((key, value, utc, source, inhibited))
                        else:
                            self._store(key, value, utc, source, inhibited)

                    if push_to_gui: 
                        asyncio.run_coroutine_threadsafe(self._broadcast(key, value, utc, source, inhibited), self._loop)
                        
                    if self.logging:
                        if self.buffer_interval and not immediate:
                            with self._buffer_lock:
                                self._log_buffer.append((utc, key, value, source, inhibited))
                        else:
                            self.csv_writer.writerow((utc, key, value, source, inhibited))
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print(f"[TelemetryServer] Push Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
            if element != None:
                if element.element_class == 'Actuator':
                    if element.control_type == 'selector':
                        for i in range(len(element.states_index)):
                            element.states_index[i].value = False
                        element.states_index[value].value = True
                        inhibited = [any(not bool(elem.value) for elem in state.armed_by) or any(bool(elem.value) for elem in state.disarmed_by) or element.states_index[value] not in state.transition_from for state in element.states_index]
                        
                    if element.element_class in ['State', 'Actuator']: # Write preactuation state to show actuation on a graph as a step instead of a long slope
                        _push(utc-1, key, element.value, source, False)
                    
                element.value = value
            _push(utc, key, value, source, push_to_gui=push_to_gui, inhibited=inhibited)
            
            
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[TelemetryServer] Send Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")


    async def _broadcast(self, key, value, utc, source=None, inhibited=None):
        point = json.dumps({"key": key, "value": value, "utc": utc, "source": source, "inhibited": inhibited})
        for client in list(self._clients):
            try:
                await client.send_text(point)
            except Exception:
                if client in self._clients:
                    self._clients.remove(client)

    # ---------------- lifecycle (mirrors OpenMCTServer) ----------------

    def start(self):
        try:
            if self.is_running:
                print("[TelemetryServer] is already running.")
                return

            config = uvicorn.Config(
                self.app,
                host="0.0.0.0",
                port=self.port,
                log_config=None,     # stop uvicorn from touching the shared/global logging config
                access_log=False,    # suppress its built-in per-request access log
            )
            self._server = uvicorn.Server(config)

            def _run():
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
                self._loop.run_until_complete(self._server.serve())

            self._thread = threading.Thread(target=_run, daemon=True)
            self._thread.start()
            while self._loop is None:
                time.sleep(0.01)

            if self.buffer_interval:
                self._flush_stop = threading.Event()
                self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
                self._flush_thread.start()

            print(f"[TelemetryServer] started on port {self.port}")
            
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[TelemetryServer] Start Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

    def stop(self, delete_db=False):
        try:
            if not self.is_running:
                print("[TelemetryServer] is not running.")
                return

            if self._flush_thread:
                self._flush_stop.set()
                self._flush_thread.join(timeout=2)
                self._flush_thread = None
            self._server.should_exit = True
            self._thread.join(timeout=5)
            self._server = None
            self._loop = None
            
            with self._db_lock:
                if self._db_conn:
                    self._db_conn.close()
                    self._db_conn = None

            if delete_db and os.path.exists(self.db_path):
                os.remove(self.db_path)
            
            print("[TelemetryServer] stopped.")
            
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[TelemetryServer] Stop Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

    def restart(self):
        self.stop()
        self.start()

    @property
    def is_running(self):
        return self._server is not None and self._thread is not None and self._thread.is_alive()