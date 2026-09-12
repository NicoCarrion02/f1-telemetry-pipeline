import os
import sys
import shutil
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

from config.settings import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_DESTINATION_FOLDER,
    KAGGLE_DATASET_NAME,
    LOCAL_RAW_PATH,
    TARGET_FILES,
    DEFAULT_REPLAY_YEAR,
    DEFAULT_REPLAY_RACE,
    DEFAULT_REPLAY_SESSION,
)
from producer.main import F1TelemetryProducer

load_dotenv()


def download_kaggle_dataset(
    dataset_name: str = KAGGLE_DATASET_NAME,
    download_path: str = LOCAL_RAW_PATH,
    target_files: list = TARGET_FILES,
) -> bool:
    """
    Paso 1: Descarga el dataset histórico de Formula 1 desde Kaggle (batch)
    y copia los archivos necesarios (lap_times, drivers, races, circuits) a data/raw.
    """
    print("\n" + "=" * 70)
    print(" PASO 1: DESCARGA DE DATOS BATCH (KAGGLE)")
    print("=" * 70)
    print(f"Descargando dataset '{dataset_name}' vía kagglehub...")

    try:
        import kagglehub
    except ImportError:
        print("Error: 'kagglehub' no está instalado. Instálalo con 'pip install kagglehub'.")
        return False

    try:
        ruta_cache = kagglehub.dataset_download(dataset_name)
        print(f"Dataset descargado en caché local: {ruta_cache}")

        os.makedirs(download_path, exist_ok=True)

        for file_name in target_files:
            source = os.path.join(ruta_cache, file_name)
            destination = os.path.join(download_path, file_name)

            if os.path.exists(source):
                shutil.copy(source, destination)
                print(f" [OK] Copiado: {file_name} -> {destination}")
            else:
                print(f" [!] Advertencia: {file_name} no encontrado en la descarga.")

        print("Descarga y estructuración de archivos batch completada.")
        return True
    except Exception as e:
        print(f"Error al descargar dataset de Kaggle: {e}")
        return False


def upload_to_gcs(
    bucket_name: str = GCS_BUCKET_NAME,
    source_folder: str = LOCAL_RAW_PATH,
    destination_folder: str = GCS_DESTINATION_FOLDER,
    target_files: list = TARGET_FILES,
) -> bool:
    """
    Paso 2: Sube los archivos batch a Google Cloud Storage Data Lake.
    """
    print("\n" + "=" * 70)
    print(f" PASO 2: CARGA DE ARCHIVOS BATCH A GCS (gs://{bucket_name}/{destination_folder})")
    print("=" * 70)

    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        for file_name in target_files:
            local_file_path = os.path.join(source_folder, file_name)

            if os.path.exists(local_file_path):
                blob_path = f"{destination_folder}{file_name}"
                blob = bucket.blob(blob_path)
                print(f" [Subiendo] {file_name} -> gs://{bucket_name}/{blob_path} ...")
                blob.upload_from_filename(local_file_path)
                print(f" [OK] Subido exitosamente: {file_name}")
            else:
                print(f" [!] Advertencia: {local_file_path} no existe localmente.")

        print("Carga a GCS finalizada exitosamente.")
        return True
    except Exception as e:
        print(f"Aviso/Error al conectar con Google Cloud Storage: {e}")
        print("Continuando con el flujo de streaming...")
        return False


def stream_all_drivers_telemetry(
    year: int = DEFAULT_REPLAY_YEAR,
    race: str = DEFAULT_REPLAY_RACE,
    session: str = DEFAULT_REPLAY_SESSION,
    delay: float = 0.2,
):
    """
    Paso 3: Transmite en tiempo real la telemetría de TODOS los pilotos de TODAS
    las escuderías (Ferrari, Red Bull, Mercedes, McLaren, etc.), intercalada
    cronológicamente para alimentar el pipeline en streaming (Pub/Sub -> Spark -> BigQuery).
    """
    print("\n" + "=" * 70)
    print(" PASO 3: STREAMING EN TIEMPO REAL - TODOS LOS PILOTOS Y ESCUDERÍAS")
    print(f" Gran Premio: {race} {year} | Sesión: {session}")
    print("=" * 70)

    producer = F1TelemetryProducer()
    # 'all' indica cargar todos los pilotos y escuderías presentes en la carrera
    producer.run_replay(
        year=year,
        race=race,
        session_type=session,
        drivers="all",
        delay=delay,
    )


def run_pipeline():
    """
    Orquestador principal sin necesidad de argumentos:
    1. Descarga datos batch de Kaggle.
    2. Sube los archivos batch al bucket de GCS.
    3. Inicia la transmisión en tiempo real de todos los pilotos y empresas hacia la base de datos.
    """
    print("*" * 70)
    print("       INICIANDO PIPELINE DE TELEMETRÍA DE FORMULA 1")
    print("*" * 70)

    # 1. Descarga datos batch
    download_kaggle_dataset()

    # 2. Carga a Data Lake en GCS
    upload_to_gcs()

    # 3. Streaming en tiempo real de todos los pilotos
    stream_all_drivers_telemetry()

    print("\n" + "*" * 70)
    print("       PIPELINE EJECUTADO CON ÉXITO")
    print("*" * 70)


if __name__ == "__main__":
    run_pipeline()
