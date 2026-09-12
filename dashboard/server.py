import os
import sys
import json
import asyncio
from pathlib import Path
from typing import List, Dict, Any

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from config.settings import (
    GCP_PROJECT_ID,
    BIGQUERY_DATASET,
    BIGQUERY_TABLE,
    BIGQUERY_EMULATOR_HOST,
)

load_dotenv()

app = FastAPI(title="F1 Telemetry Real-Time Cockpit Dashboard")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Driver and Constructor Metadata
CONSTRUCTOR_COLORS = {
    "Red Bull Racing": "#3671C6",
    "Ferrari": "#E80020",
    "Mercedes": "#27F4D2",
    "McLaren": "#FF8000",
    "Aston Martin": "#229971",
    "Alpine": "#0093CC",
    "Williams": "#64C4FF",
    "AlphaTauri": "#5E8FAA",
    "RB": "#6692FF",
    "Alfa Romeo": "#C92D4B",
    "Kick Sauber": "#52E252",
    "Haas F1 Team": "#B6BABD",
    "Unknown Team": "#FFFFFF"
}

DRIVER_ROSTER = {
    "1": {"code": "VER", "name": "Max Verstappen", "team": "Red Bull Racing", "color": "#3671C6"},
    "11": {"code": "PER", "name": "Sergio Perez", "team": "Red Bull Racing", "color": "#3671C6"},
    "16": {"code": "LEC", "name": "Charles Leclerc", "team": "Ferrari", "color": "#E80020"},
    "55": {"code": "SAI", "name": "Carlos Sainz", "team": "Ferrari", "color": "#E80020"},
    "44": {"code": "HAM", "name": "Lewis Hamilton", "team": "Mercedes", "color": "#27F4D2"},
    "63": {"code": "RUS", "name": "George Russell", "team": "Mercedes", "color": "#27F4D2"},
    "4": {"code": "NOR", "name": "Lando Norris", "team": "McLaren", "color": "#FF8000"},
    "81": {"code": "PIA", "name": "Oscar Piastri", "team": "McLaren", "color": "#FF8000"},
    "14": {"code": "ALO", "name": "Fernando Alonso", "team": "Aston Martin", "color": "#229971"},
    "18": {"code": "STR", "name": "Lance Stroll", "team": "Aston Martin", "color": "#229971"},
    "10": {"code": "GAS", "name": "Pierre Gasly", "team": "Alpine", "color": "#0093CC"},
    "31": {"code": "OCO", "name": "Esteban Ocon", "team": "Alpine", "color": "#0093CC"},
    "23": {"code": "ALB", "name": "Alexander Albon", "team": "Williams", "color": "#64C4FF"},
    "2": {"code": "SAR", "name": "Logan Sargeant", "team": "Williams", "color": "#64C4FF"},
    "77": {"code": "BOT", "name": "Valtteri Bottas", "team": "Alfa Romeo", "color": "#C92D4B"},
    "24": {"code": "ZHO", "name": "Guanyu Zhou", "team": "Alfa Romeo", "color": "#C92D4B"},
    "20": {"code": "MAG", "name": "Kevin Magnussen", "team": "Haas F1 Team", "color": "#B6BABD"},
    "27": {"code": "HUL", "name": "Nico Hulkenberg", "team": "Haas F1 Team", "color": "#B6BABD"},
    "22": {"code": "TSU", "name": "Yuki Tsunoda", "team": "AlphaTauri", "color": "#5E8FAA"},
    "40": {"code": "LAW", "name": "Liam Lawson", "team": "AlphaTauri", "color": "#5E8FAA"},
}


class ConnectionManager:
    """Manages active WebSocket connections to broadcast real-time telemetry."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        if not self.active_connections:
            return
        msg_str = json.dumps(message)
        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(msg_str)
            except Exception:
                dead_connections.append(connection)
        for dead in dead_connections:
            self.disconnect(dead)


manager = ConnectionManager()

# Mount static files directory
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def get_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return HTMLResponse(content=index_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>F1 Telemetry Dashboard Loading...</h1>")


@app.get("/api/health")
async def health_check():
    return {
        "status": "online",
        "active_ws_clients": len(manager.active_connections),
        "bigquery_target": f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        "emulator_host": BIGQUERY_EMULATOR_HOST or "cloud"
    }


@app.get("/api/drivers")
async def get_drivers():
    return DRIVER_ROSTER


@app.post("/api/telemetry/ingest")
async def ingest_telemetry(payload: Dict[str, Any]):
    """
    HTTP Ingestion webhook:
    Permite que cualquier componente del pipeline envíe directamente eventos de telemetría
    para ser transmitidos inmediatamente vía WebSocket a los navegadores.
    """
    driver_str = str(payload.get("driver_number", ""))
    driver_info = DRIVER_ROSTER.get(driver_str, {})
    
    enriched_payload = {
        **payload,
        "driver_code": driver_info.get("code", f"D{driver_str}"),
        "driver_name": driver_info.get("name", f"Driver {driver_str}"),
        "team_name": payload.get("team_name") or driver_info.get("team", "Formula 1"),
        "team_color": driver_info.get("color", CONSTRUCTOR_COLORS.get(driver_info.get("team", ""), "#FFFFFF"))
    }
    
    await manager.broadcast(enriched_payload)
    return {"status": "broadcasted"}


@app.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Enviar mensaje de bienvenida con el estado
        await websocket.send_json({
            "type": "connection_ack",
            "message": "Connected to F1 Real-Time Telemetry Stream",
            "drivers": DRIVER_ROSTER
        })
        while True:
            # Mantener conexión viva y recibir posibles mensajes del cliente (ej. cambio de piloto filtrado)
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


# Tarea de fondo para consultar BigQuery periódicamente (si hay datos disponibles)
async def bigquery_poller_task():
    """Consulta las filas más recientes de BigQuery y las transmite si no hay stream activo."""
    if not GCP_PROJECT_ID:
        return

    try:
        from google.cloud import bigquery
        bq_client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
        last_timestamp = None

        while True:
            await asyncio.sleep(2.0)
            if not manager.active_connections:
                continue

            try:
                query = f"""
                    SELECT timestamp, session_id, driver_number, speed_kmh, rpm, gear, throttle, brake, x_pos, y_pos, nombre, apellido, nacionalidad
                    FROM `{table_id}`
                    ORDER BY timestamp DESC
                    LIMIT 20
                """
                query_job = bq_client.query(query)
                results = list(query_job.result())
                
                # Transmitir en orden cronológico ascendente los más recientes
                for row in reversed(results):
                    row_dict = dict(row.items())
                    row_dict["timestamp"] = str(row_dict["timestamp"])
                    driver_num = str(row_dict.get("driver_number", ""))
                    meta = DRIVER_ROSTER.get(driver_num, {})
                    row_dict["driver_code"] = meta.get("code", f"D{driver_num}")
                    row_dict["driver_name"] = f"{row_dict.get('nombre', '')} {row_dict.get('apellido', '')}".strip() or meta.get("name", f"Driver {driver_num}")
                    row_dict["team_name"] = meta.get("team", "Formula 1")
                    row_dict["team_color"] = meta.get("color", "#FFFFFF")
                    
                    await manager.broadcast(row_dict)
                    await asyncio.sleep(0.05)
            except Exception:
                # Tabla aún vacía o BigQuery no inicializado todavía
                pass
    except Exception:
        pass


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(bigquery_poller_task())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard.server:app", host="0.0.0.0", port=8000, reload=True)
