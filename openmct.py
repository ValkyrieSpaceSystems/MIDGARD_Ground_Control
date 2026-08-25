import os, threading, asyncio, json, sqlite3, time, uvicorn, signal, re
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from queue import Queue

from utility import get_element


def _safe_key(*parts):
    """Join parts into a stable, collision-safe identifier key."""
    return ".".join(re.sub(r"[^a-zA-Z0-9_-]", "_", str(p)) for p in parts)

def buildOpenMCTjs(peripherals, output_path=None):
    output_path = output_path or os.path.join(
        os.path.dirname(__file__), "openmct_midgard", "telemetry-tree.js"
    )

    type_lookup = {
        'radio': 'Radio', 'servo': 'Servo', 'fin': 'Fin', 'tvc': 'TVC', 'pyro': 'Pyro',
        'position': 'Position', 'velocity': 'Velocity', 'acceleration': 'Acceleration',
        'attitude': 'Attitude', 'heading': 'Heading', 'pressure': 'Pressure', 'gps': 'GPS',
    }

    def label(raw):
        return type_lookup.get(raw, raw.replace('_', ' ').title())

    data_tree = {}
    all_keys = []
    peripheral_folders = []

    for peripheral_name, peripheral in peripherals.items():
        data_tree[peripheral_name] = {}
        element_folders = []

        element_lookup = {
            #'Interfaces': peripheral.interfaces,
            #'Phases': peripheral.phases,
            'Actuators': peripheral.actuators,
            'Data_Streams': peripheral.data_streams,
        }

        for element_type, element_dict in element_lookup.items():
            if not element_dict:
                continue
            if element_type in ['Interfaces', 'Phases']:
                continue
            if element_type != 'Data_Streams':
                pass

            data_tree[peripheral_name][element_type] = {}

            # group leaves by (type, subtype) — either or both may be None
            groups = {}
            for element_id, element in element_dict.items():
                data_tree[peripheral_name][element_type][element_id] = element

                comp_type = getattr(element, 'type', None)
                comp_subtype = getattr(element, 'subtype', None)

                key = _safe_key(peripheral_name, element_type, element_id)
                element.key = key
                all_keys.append(key)
                
                states = getattr(element, 'states', None)
                if states:
                    meas_format = {'format': 'enum', 'enumerations': [{'value': i, 'string': str(s)} for i, s in enumerate(states)]}
                elif element.unit == 'bool':
                    meas_format = {'format': 'enum', 'enumerations': [{'value': 0, 'string': element.nominal_state}, {'value': 1, 'string': element.off_nominal_state}]}
                elif element.unit == 'str':
                    meas_format = {'units': element.unit, 'format': 'string'}
                else:
                    meas_format = {'units': element.unit, 'format': 'number'}
                    
                selected_color = getattr(element, 'selected_color', None)
                unselected_color = getattr(element, 'unselected_color', None)
                if selected_color or unselected_color:
                    meas_format['style'] = {'selected': selected_color, 'unselected': unselected_color}

                switch_display = {}
                switch_display.update(getattr(peripheral, 'switch_display', {}) or {})
                switch_display.update(getattr(element, 'switch_display', {}) or {})
                
                leaf = {"name": element.name, "key": key, "measurement": meas_format}
                if switch_display:
                    leaf["switch_display"] = switch_display
                groups.setdefault((comp_type, comp_subtype), []).append(leaf)

            # fold groups into a folder tree: [type folder ->] [subtype folder ->] leaves
            type_buckets = {}   # type_or_None -> {"leaves": [...], "subtypes": {subtype: [...]}}
            for (comp_type, comp_subtype), leaves in groups.items():
                bucket = type_buckets.setdefault(comp_type, {"leaves": [], "subtypes": {}})
                if comp_subtype:
                    bucket["subtypes"].setdefault(comp_subtype, []).extend(leaves)
                else:
                    bucket["leaves"].extend(leaves)

            element_children = []
            for comp_type, bucket in type_buckets.items():
                if comp_type is None:
                    # no type at all — sits directly in the element-type folder
                    element_children.extend(bucket["leaves"])
                    for subtype_name, sub_leaves in bucket["subtypes"].items():
                        element_children.append({
                            "name": label(subtype_name),
                            "key": _safe_key(peripheral_name, element_type, "untyped", subtype_name),
                            "children": sub_leaves,
                        })
                    continue

                type_folder_children = list(bucket["leaves"])
                for subtype_name, sub_leaves in bucket["subtypes"].items():
                    type_folder_children.append({
                        "name": label(subtype_name),
                        "key": _safe_key(peripheral_name, element_type, comp_type, subtype_name),
                        "children": sub_leaves,
                    })

                element_children.append({
                    "name": label(comp_type),
                    "key": _safe_key(peripheral_name, element_type, comp_type),
                    "children": type_folder_children,
                })

            if element_children:
                element_folders.append({
                    "name": element_type,
                    "key": _safe_key(peripheral_name, element_type),
                    "children": element_children,
                })

        peripheral_folders.append({
            "name": peripheral.display_name,
            "key": _safe_key(peripheral_name),
            "children": element_folders,
        })

    tree = {"name": "MIDGARD", "key": "midgard_root", "children": peripheral_folders}
    js = "// Auto-generated by buildOpenMCTjs() — do not edit by hand.\n"
    js += "var TELEMETRY_TREE = " + json.dumps(tree, indent=4) + ";\n"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(js)

    #print(data_tree, all_keys)
    return data_tree, all_keys


class OpenMCTServer:
    def __init__(self, stop_event, port=4000, show_logs=False,
        openmct_dir=os.path.join(os.path.dirname(__file__), "openmct"),
        static_dir=os.path.join(os.path.dirname(__file__), "openmct_midgard"),
    ):
        self.port = port
        self.show_logs = show_logs
        self.dist_dir = os.path.join(openmct_dir, "dist")
        self.static_dir = static_dir  # your index.html, dictionary.js, plugin files

        if not os.path.isdir(self.dist_dir):
            raise FileNotFoundError(
                f"{self.dist_dir} not found — run `npm run build` inside {openmct_dir} first"
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

    def stop(self):
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
    def __init__(self, stop_event, port=4001, db_path=None, show_logs=False, buffer_interval=0.2):
        self.stop_event = stop_event
        self.port = port
        self.db_path = db_path or os.path.join(os.path.dirname(__file__), "telemetry.db")
        self.show_logs = show_logs
        self.buffer_interval = buffer_interval   # seconds; None = always write immediately
        self._db_lock = threading.Lock()
        self._db_conn = None
        self._write_buffer = []
        self._buffer_lock = threading.Lock()
        self._flush_thread = None
        self._flush_stop = None

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
                "CREATE TABLE IF NOT EXISTS telemetry (key TEXT NOT NULL, value REAL, utc INTEGER NOT NULL, source TEXT)"
            )
            existing_cols = [row[1] for row in self._db_conn.execute("PRAGMA table_info(telemetry)").fetchall()]
            if "source" not in existing_cols:
                self._db_conn.execute("ALTER TABLE telemetry ADD COLUMN source TEXT")
            self._db_conn.execute("CREATE INDEX IF NOT EXISTS idx_key_utc ON telemetry(key, utc)")
            self._db_conn.commit()

    def _store(self, key, value, utc, source=None):
        with self._db_lock:
            self._db_conn.execute(
                "INSERT INTO telemetry (key, value, utc, source) VALUES (?, ?, ?, ?)", (key, value, utc, source)
            )
            self._db_conn.commit()

    def _query_history(self, key, start, end):
        with self._db_lock:
            rows = self._db_conn.execute(
                "SELECT value, utc, source FROM telemetry WHERE key = ? AND utc BETWEEN ? AND ? ORDER BY utc",
                (key, start, end),
            ).fetchall()
        return [{"key": key, "value": v, "utc": u, "source": s} for v, u, s in rows]
    
    def _flush_buffer(self):
        with self._buffer_lock:
            if not self._write_buffer:
                return
            batch, self._write_buffer = self._write_buffer, []
        with self._db_lock:
            self._db_conn.executemany(
                "INSERT INTO telemetry (key, value, utc, source) VALUES (?, ?, ?, ?)", batch
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
                        self.command_queue.put_nowait({"key": msg["key"], "requested": msg["requested"]})
            except WebSocketDisconnect:
                pass
            finally:
                if websocket in self._clients:
                    self._clients.remove(websocket)
                if self.show_logs:
                    print(f"[TelemetryServer] client disconnected ({len(self._clients)} clients)")

    # ---------------- publish data ----------------

    def send(self, key: str, value, data_tree, source: str, immediate=False, push_to_gui=True):
        if isinstance(value, bool):
            value = int(value)
        element = get_element(data_tree, key.split('.'))
        element.value = value
        if not self.is_running:
            print('[TelemetryServer] Cannot send data because server is not running')
            return
        utc = int(time.time() * 1000)
        if self.buffer_interval and not immediate:
            with self._buffer_lock:
                self._write_buffer.append((key, value, utc, source))
        else:
            self._store(key, value, utc, source)

        if push_to_gui: 
            asyncio.run_coroutine_threadsafe(self._broadcast(key, value, utc, source), self._loop)

    async def _broadcast(self, key, value, utc, source=None):
        point = json.dumps({"key": key, "value": value, "utc": utc, "source": source})
        for client in list(self._clients):
            try:
                await client.send_text(point)
            except Exception:
                if client in self._clients:
                    self._clients.remove(client)

    # ---------------- receive commands ----------------

    def receive(self, handler):
        self._command_handler = handler

    # ---------------- lifecycle (mirrors OpenMCTServer) ----------------

    def start(self):
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

    def stop(self):
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
        
        print("[TelemetryServer] stopped.")

    def restart(self):
        self.stop()
        self.start()

    @property
    def is_running(self):
        return self._server is not None and self._thread is not None and self._thread.is_alive()