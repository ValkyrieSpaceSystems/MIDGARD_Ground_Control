import os, threading, asyncio, json, sqlite3, time, uvicorn, signal
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


class OpenMCTServer:
    def __init__(self, port=4000, show_logs=False,
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
    def __init__(self, port=4001, db_path=None, show_logs=False):
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

    def send(self, key: str, value):
        """Thread-safe. Call this whenever a new telemetry value arrives."""
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