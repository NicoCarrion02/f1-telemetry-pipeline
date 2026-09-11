import os
import time
import json
import argparse
import fastf1
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

# Cargar variables de entorno
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUB_SUB_TOPIC")

class F1TelemetryProducer:
    def __init__(self):
        # Inicializar el cliente de Pub/Sub
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)
        print(f"Conectado a Pub/Sub: {self.topic_path}")

    def publish_event(self, payload):
        """Convierte el diccionario a JSON y lo publica en GCP Pub/Sub."""
        data_str = json.dumps(payload)
        data_bytes = data_str.encode("utf-8")
        
        future = self.publisher.publish(self.topic_path, data_bytes)
        print(f"Publicado [MsgID: {future.result()}]: {data_str}")

    def run_replay(self, year, race, session_type, driver):
        """Modo Simulación: Descarga telemetría histórica de FastF1 y la emite con delay."""
        print(f"Cargando sesión histórica: {year} {race} ({session_type}) - Piloto: {driver}")
        
        fastf1.Cache.enable_cache('data/raw')
        
        session = fastf1.get_session(year, race, session_type)
        session.load(telemetry=True, laps=True, weather=False)
        
        laps = session.laps.pick_driver(driver)
        telemetry = laps.get_telemetry()
        
        print("Iniciando transmisión de telemetría simulada...")
        
        for index, row in telemetry.iterrows():
            payload = {
                "timestamp": str(row['Date']),
                "session_id": f"{year}{race[:3].upper()}",
                "driver_number": driver,
                "speed_kmh": int(row['Speed']),
                "rpm": int(row['RPM']),
                "gear": int(row['nGear']),
                "throttle": int(row['Throttle']),
                "brake": int(row['Brake']),
                "x_pos": int(row['X']),
                "y_pos": int(row['Y'])
            }
            
            self.publish_event(payload)
            # Delay de 0.2 segundos para simular el flujo en tiempo real
            time.sleep(0.2)

    def run_live(self, driver):
        """Modo Live: Consulta la API de OpenF1 en tiempo real."""
        print(f"Iniciando monitoreo en vivo para el piloto {driver}...")
        # Endpoint de ejemplo, la estructura exacta depende de la carrera activa
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
                            "driver_number": record['driver_number'],
                            "speed_kmh": record['speed'],
                            "rpm": record['rpm'],
                            "gear": record['n_gear'],
                            "throttle": record['throttle'],
                            "brake": record['brake'],
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
    parser.add_argument("--mode", choices=["live", "replay"], required=True, help="Modo de ejecución: live o replay")
    parser.add_argument("--driver", type=str, default="16", help="Número del piloto (ej. 16 para Leclerc)")
    args = parser.parse_args()

    producer = F1TelemetryProducer()

    if args.mode == "replay":
        # Por defecto simulará a Charles Leclerc (16) en Monza 2023
        producer.run_replay(2023, 'Monza', 'R', args.driver)
    elif args.mode == "live":
        producer.run_live(args.driver)