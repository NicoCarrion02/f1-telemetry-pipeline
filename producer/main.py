import os
import sys
import time
import json
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Add project root to sys.path if not already present
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import fastf1
import pandas as pd
import requests
from dotenv import load_dotenv

from config.settings import (
    GCP_PROJECT_ID,
    PUB_SUB_TOPIC,
    DEFAULT_REPLAY_YEAR,
    DEFAULT_REPLAY_RACE,
    DEFAULT_REPLAY_SESSION,
    DEFAULT_DRIVERS,
    LOCAL_RAW_PATH,
)

# Load environment variables
load_dotenv()
PROJECT_ID = GCP_PROJECT_ID or os.getenv("GCP_PROJECT_ID", "")
TOPIC_ID = PUB_SUB_TOPIC or os.getenv("PUB_SUB_TOPIC", "f1-telemetry-topic")


class F1TelemetryProducer:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.publisher = None
        self.topic_path = None

        if not self.dry_run and PROJECT_ID and TOPIC_ID:
            try:
                from google.cloud import pubsub_v1
                self.publisher = pubsub_v1.PublisherClient()
                self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)
                print(f"Conectado a Pub/Sub: {self.topic_path}")
            except Exception as e:
                print(f"Aviso: No se pudo conectar a Pub/Sub ({e}). Operando en modo dry-run (simulación local).")
                self.dry_run = True
        else:
            if not self.dry_run:
                print("Aviso: GCP_PROJECT_ID o PUB_SUB_TOPIC no configurados. Operando en modo dry-run.")
        self.dashboard_url = os.getenv("DASHBOARD_INGEST_URL", "http://localhost:8000/api/telemetry/ingest")

    def publish_event(self, payload: dict):
        """Convierte el diccionario a JSON y lo publica en GCP Pub/Sub o en consola si es dry-run."""
        data_str = json.dumps(payload)
        
        # Enviar copia al dashboard en tiempo real si está activo
        if self.dashboard_url:
            try:
                requests.post(self.dashboard_url, json=payload, timeout=0.08)
            except Exception:
                pass

        if self.dry_run or not self.publisher:
            print(f"[DRY RUN EMIT]: {data_str}")
            return None

        data_bytes = data_str.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data_bytes)
        msg_id = future.result()
        print(f"Publicado [MsgID: {msg_id}]: {data_str}")
        return msg_id

    def load_replay_session(self, year: int, race: str, session_type: str):
        """Carga y cachea la sesión histórica de FastF1."""
        print(f"Cargando sesión histórica FastF1: {year} {race} ({session_type})...")
        os.makedirs(LOCAL_RAW_PATH, exist_ok=True)

        # Asegurar descarga de archivos .ff1pkl desde GCS si no están presentes (entorno Cloud)
        try:
            from jobs.seed import sync_fastf1_cache
            sync_fastf1_cache(source_folder=LOCAL_RAW_PATH)
        except Exception as e:
            print(f"Aviso verificando caché GCS: {e}")

        fastf1.Cache.enable_cache(LOCAL_RAW_PATH)

        session = fastf1.get_session(year, race, session_type)
        session.load(telemetry=True, laps=True, weather=False)
        return session

    def get_session_driver_team_map(self, session) -> dict:
        """Extrae el mapeo de pilotos a escuderías (empresas/constructores)."""
        driver_team_map = {}
        if hasattr(session, 'results') and session.results is not None and not session.results.empty:
            for _, row in session.results.iterrows():
                num = str(row.get('DriverNumber', '')).strip()
                team = str(row.get('TeamName', 'Unknown Team')).strip()
                abbr = str(row.get('Abbreviation', '')).strip()
                driver_team_map[num] = {"team": team, "abbr": abbr}
                if abbr:
                    driver_team_map[abbr] = {"team": team, "abbr": abbr, "number": num}
        return driver_team_map

    def run_replay(
        self,
        year: int = DEFAULT_REPLAY_YEAR,
        race: str = DEFAULT_REPLAY_RACE,
        session_type: str = DEFAULT_REPLAY_SESSION,
        drivers: list = None,
        delay: float = 0.05,
        limit_records: int = None,
        concurrent: bool = False
    ):
        """
        Modo Simulación: Descarga telemetría histórica de FastF1 y la emite para múltiples pilotos y escuderías.
        
        Si concurrent=False, combina la telemetría de todos los pilotos ordenándola cronológicamente,
        simulando la carrera en tiempo real sincronizado entre todas las escuderías.
        """
        session = self.load_replay_session(year, race, session_type)

        if not drivers or drivers == "all" or drivers == ["all"]:
            drivers = list(session.drivers)
            print(f"Detectados {len(drivers)} pilotos de todas las escuderías en la sesión.")
        else:
            # Convertir elementos de drivers a string
            drivers = [str(d).strip() for d in drivers]

        driver_meta = self.get_session_driver_team_map(session)

        print(f"\n--- Iniciando Replay para {len(drivers)} pilotos en {race} {year} ---")
        for d in drivers:
            info = driver_meta.get(d, {})
            team = info.get("team", "Equipo No Identificado")
            print(f" - Piloto #{d}: {team}")

        if concurrent:
            self._run_replay_concurrent(session, drivers, year, race, delay, limit_records)
        else:
            self._run_replay_interleaved(session, drivers, year, race, delay, limit_records)

    def _build_payload(self, row, driver_str: str, year: int, race: str) -> dict:
        """Construye el payload compatible con el pipeline de streaming Spark/BigQuery."""
        try:
            driver_num = int(driver_str)
        except ValueError:
            driver_num = driver_str

        return {
            "timestamp": str(row['Date']),
            "session_id": f"{year}{race[:3].upper()}",
            "driver_number": driver_num,
            "speed_kmh": int(row.get('Speed', 0) or 0),
            "rpm": int(row.get('RPM', 0) or 0),
            "gear": int(row.get('nGear', 0) or 0),
            "throttle": int(row.get('Throttle', 0) or 0),
            "brake": int(row.get('Brake', 0) or 0),
            "x_pos": int(row.get('X', 0) or 0),
            "y_pos": int(row.get('Y', 0) or 0)
        }

    def _run_replay_interleaved(self, session, drivers: list, year: int, race: str, delay: float, limit_records: int):
        """Combina y ordena cronológicamente la telemetría de todos los pilotos."""
        print("\nRecopilando telemetría de todos los pilotos seleccionados...")
        driver_telemetry_frames = []

        for driver in drivers:
            try:
                if hasattr(session.laps, 'pick_drivers'):
                    laps = session.laps.pick_drivers(driver)
                else:
                    laps = session.laps.pick_driver(driver)

                if laps.empty:
                    print(f"Advertencia: No se encontraron vueltas para el piloto {driver}.")
                    continue
                telem = laps.get_telemetry()
                if telem.empty:
                    print(f"Advertencia: Telemetría vacía para el piloto {driver}.")
                    continue

                telem = telem.copy()
                telem['target_driver'] = driver
                driver_telemetry_frames.append(telem)
                print(f"Cargadas {len(telem)} muestras de telemetría para el piloto {driver}.")
            except Exception as e:
                print(f"Error extrayendo telemetría del piloto {driver}: {e}")

        if not driver_telemetry_frames:
            print("Aviso: No se pudo obtener telemetría de la API (bloqueo de IP de CDN en Cloud).")
            print("Iniciando generador de telemetría simulada en tiempo real para todos los pilotos...")
            self._stream_fallback_telemetry(drivers, year, race, delay, limit_records)
            return

        print("Sincronizando y ordenando telemetría cronológicamente...")
        merged_telemetry = pd.concat(driver_telemetry_frames, ignore_index=True)
        if 'Date' in merged_telemetry.columns:
            merged_telemetry.sort_values(by='Date', inplace=True)

        total_records = len(merged_telemetry)
        if limit_records and limit_records > 0:
            merged_telemetry = merged_telemetry.head(limit_records)
            print(f"Limitando transmisión a {limit_records} registros (de {total_records} disponibles).")
        else:
            print(f"Transmitiendo {total_records} registros intercalados en tiempo real...")

        for _, row in merged_telemetry.iterrows():
            payload = self._build_payload(row, row['target_driver'], year, race)
            self.publish_event(payload)
            if delay > 0:
                time.sleep(delay)

        print("Transmisión de telemetría simulada completada.")

    def _stream_fallback_telemetry(self, drivers: list, year: int, race: str, delay: float, limit_records: int):
        """Generador de telemetría de respaldo cuando FastF1 es bloqueado por la CDN en la nube."""
        import math
        import random
        from datetime import datetime, timezone

        print(f"Transmitiendo telemetría continua para {len(drivers)} pilotos...")
        events_sent = 0
        step = 0

        while True:
            step += 1
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            
            for idx, driver in enumerate(drivers):
                # Trayectoria en circuito (simula trazado de Monza con chicanas y rectas)
                angle = (step * 0.05 + (idx * (2 * math.pi / len(drivers)))) % (2 * math.pi)
                x = int(math.cos(angle) * 1200 + math.sin(angle * 2) * 350)
                y = int(math.sin(angle) * 2300)

                # Velocidad y marcha simuladas según recta vs curva
                is_straight = abs(math.cos(angle)) > 0.55
                speed = random.randint(285, 348) if is_straight else random.randint(85, 175)
                gear = 8 if speed > 310 else (7 if speed > 260 else (6 if speed > 210 else (4 if speed > 140 else 3)))
                rpm = int(9200 + (speed / 350) * 3100)
                throttle = 100 if is_straight else random.randint(35, 75)
                brake = 0 if is_straight else random.randint(25, 95)

                payload = {
                    "timestamp": now_str,
                    "session_id": f"{year}{race[:3].upper()}",
                    "driver_number": int(driver) if str(driver).isdigit() else driver,
                    "speed_kmh": speed,
                    "rpm": rpm,
                    "gear": gear,
                    "throttle": throttle,
                    "brake": brake,
                    "x_pos": x,
                    "y_pos": y
                }

                self.publish_event(payload)
                events_sent += 1
                if limit_records and events_sent >= limit_records:
                    print(f"Límite de {limit_records} registros alcanzado.")
                    return

            if delay > 0:
                time.sleep(delay)

    def _run_replay_concurrent(self, session, drivers: list, year: int, race: str, delay: float, limit_records: int):
        """Ejecuta la transmisión de telemetría en hilos concurrentes por piloto."""
        def _stream_single_driver(driver):
            try:
                if hasattr(session.laps, 'pick_drivers'):
                    laps = session.laps.pick_drivers(driver)
                else:
                    laps = session.laps.pick_driver(driver)
                if laps.empty:
                    return
                telem = laps.get_telemetry()
                if limit_records and limit_records > 0:
                    telem = telem.head(limit_records)

                for _, row in telem.iterrows():
                    payload = self._build_payload(row, driver, year, race)
                    self.publish_event(payload)
                    if delay > 0:
                        time.sleep(delay)
            except Exception as e:
                print(f"Error en transmisión de piloto {driver}: {e}")

        with ThreadPoolExecutor(max_workers=len(drivers)) as executor:
            executor.map(_stream_single_driver, drivers)

        print("Transmisión concurrente completada.")

    def run_live(self, driver: str):
        """Modo Live: Consulta la API de OpenF1 en tiempo real."""
        print(f"Iniciando monitoreo en vivo para el piloto {driver}...")
        base_url = f"https://api.openf1.org/v1/car_data?driver_number={driver}&session_key=latest"
        
        while True:
            try:
                response = requests.get(base_url)
                if response.status_code == 200:
                    data = response.json()
                    for record in data:
                        payload = {
                            "timestamp": record['date'],
                            "session_id": record['session_key'],
                            "driver_number": int(record['driver_number']) if str(record['driver_number']).isdigit() else record['driver_number'],
                            "speed_kmh": int(record['speed']),
                            "rpm": int(record['rpm']),
                            "gear": int(record['n_gear']),
                            "throttle": int(record['throttle']),
                            "brake": int(record['brake']),
                            "x_pos": 0,
                            "y_pos": 0
                        }
                        self.publish_event(payload)
                time.sleep(1)
            except Exception as e:
                print(f"Error consultando OpenF1: {e}")
                time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="F1 Telemetry Producer")
    parser.add_argument("--mode", choices=["live", "replay"], default="replay", help="Modo de ejecución: live o replay (por defecto: replay)")
    parser.add_argument("--year", type=int, default=DEFAULT_REPLAY_YEAR, help=f"Año de la sesión (def: {DEFAULT_REPLAY_YEAR})")
    parser.add_argument("--race", type=str, default=DEFAULT_REPLAY_RACE, help=f"Nombre del GP (def: {DEFAULT_REPLAY_RACE})")
    parser.add_argument("--session", type=str, default=DEFAULT_REPLAY_SESSION, help=f"Tipo de sesión (def: {DEFAULT_REPLAY_SESSION})")
    parser.add_argument("--driver", type=str, default=None, help="Número de un único piloto (ej: 16)")
    parser.add_argument("--drivers", type=str, default=None, help="Lista de pilotos separados por coma (ej: 1,11,16,55,44,63)")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay en segundos entre eventos (def: 0.05)")
    parser.add_argument("--limit", type=int, default=None, help="Límite máximo de eventos a emitir")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin publicar a Pub/Sub")
    parser.add_argument("--concurrent", action="store_true", help="Emitir usando hilos paralelos por piloto")
    args = parser.parse_args()

    producer = F1TelemetryProducer(dry_run=args.dry_run)

    if args.mode == "replay":
        if args.drivers:
            driver_list = [d.strip() for d in args.drivers.split(",")]
        elif args.driver:
            driver_list = [args.driver.strip()]
        else:
            driver_list = DEFAULT_DRIVERS

        producer.run_replay(
            year=args.year,
            race=args.race,
            session_type=args.session,
            drivers=driver_list,
            delay=args.delay,
            limit_records=args.limit,
            concurrent=args.concurrent
        )
    elif args.mode == "live":
        single_driver = args.driver or "16"
        producer.run_live(single_driver)