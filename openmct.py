import os, threading, asyncio, json, sqlite3, time, uvicorn, signal, re
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from utility import get_nested


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
            'Interfaces': peripheral.interfaces,
            'Phases': peripheral.phases,
            'Lockouts': peripheral.lockouts,
            'Actuators': peripheral.actuators,
            'Data_Streams': peripheral.data_streams,
        }

        for element_type, element_dict in element_lookup.items():
            if not element_dict:
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
                all_keys.append(key)
                
                unit = element.unit
                
                if unit == 'bool': meas_format = {"units": unit, 'format': 'enum', 'enumerations': [{'value': 0, 'string': 'FALSE'}, {'value': 1, 'string': 'TRUE'}]}
                elif unit == 'str': meas_format = {"units": unit, 'format': 'string'}
                else: meas_format = {"units": unit, 'format': 'number'}

                leaf = {"name": element.name, "key": key, "measurement": meas_format}
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
            print("OpenMCT server is already running.")
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

        print(f"OpenMCT server started on port {self.port}")

    def stop(self):
        if not self.is_running:
            print("OpenMCT server is not running.")
            return

        self._server.should_exit = True
        self._thread.join(timeout=5)
        self._server = None
        self._thread = None
        print("OpenMCT server stopped.")

    def restart(self):
        self.stop()
        self.start()

    @property
    def is_running(self):
        return self._thread is not None and self._thread.is_alive()

    
class TelemetryServer:
    def __init__(self, stop_event, port=4001, db_path=None, show_logs=False):
        self.port = port
        self.db_path = db_path or os.path.join(os.path.dirname(__file__), "telemetry.db")
        self.show_logs = show_logs

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
        self._command_handler = None

        self._init_db()
        self._register_routes()

    # ---------------- storage ----------------

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS telemetry (key TEXT NOT NULL, value REAL, utc INTEGER NOT NULL)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_key_utc ON telemetry(key, utc)")
        conn.commit()
        conn.close()

    def _store(self, key, value, utc):
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO telemetry (key, value, utc) VALUES (?, ?, ?)", (key, value, utc))
        conn.commit()
        conn.close()

    def _query_history(self, key, start, end):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(
            "SELECT value, utc FROM telemetry WHERE key = ? AND utc BETWEEN ? AND ? ORDER BY utc",
            (key, start, end),
        ).fetchall()
        conn.close()
        return [{"key": key, "value": v, "utc": u} for v, u in rows]

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
                        print(msg)
                    except json.JSONDecodeError:
                        continue
                    
                # change to fit data receive
                # -----------------------------------------------
                    if "key" in msg and "requested" in msg and self._command_handler:  
                        final_value = self._command_handler(msg["key"], msg["requested"])
                        self.publish(msg["key"], final_value)  # reuses your existing broadcast logic
                        
                # -----------------------------------------------
                    
                    
            except WebSocketDisconnect:
                pass
            finally:
                if websocket in self._clients:
                    self._clients.remove(websocket)
                if self.show_logs:
                    print(f"[TelemetryServer] client disconnected ({len(self._clients)} clients)")

    # ---------------- publish data ----------------

    def send(self, key: str, value, data_tree):
        element = get_nested(data_tree, key.split('.'))
        element.value = value
        if not self.is_running:
            print('[TelemetryServer] Cannot send data because server is not running')
            return
        utc = int(time.time() * 1000)
        self._store(key, value, utc)
        asyncio.run_coroutine_threadsafe(self._broadcast(key, value, utc), self._loop)

    async def _broadcast(self, key, value, utc):
        point = json.dumps({"key": key, "value": value, "utc": utc})
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
            print("Telemetry server is already running.")
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

        while self._loop is None:      # wait for the loop to actually spin up
            time.sleep(0.01)

        print(f"Telemetry server started on port {self.port}")

    def stop(self):
        if not self.is_running:
            print("Telemetry server is not running.")
            return

        self._server.should_exit = True
        self._thread.join(timeout=5)
        self._server = None
        self._loop = None
        print("Telemetry server stopped.")

    def restart(self):
        self.stop()
        self.start()

    @property
    def is_running(self):
        return self._server is not None and self._thread is not None and self._thread.is_alive()