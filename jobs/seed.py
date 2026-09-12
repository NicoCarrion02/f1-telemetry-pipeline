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
    BIGQUERY_DATASET,
    DEFAULT_REPLAY_YEAR,
    DEFAULT_REPLAY_RACE,
    DEFAULT_REPLAY_SESSION,
)
from data.batch_to_bigquery import process_and_load_batch_data
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
        print("Continuando con el flujo...")
        return False


def load_batch_to_bigquery(
    project_id: str = GCP_PROJECT_ID,
    bucket_name: str = GCS_BUCKET_NAME,
    dataset_name: str = BIGQUERY_DATASET,
    target_files: list = TARGET_FILES,
) -> bool:
    """
    Paso 3: Procesa, limpia y carga los archivos batch desde GCS a BigQuery.
    """
    print("\n" + "=" * 70)
    print(" PASO 3: ETL BATCH A BIGQUERY (GCS -> BIGQUERY)")
    print("=" * 70)

    try:
        process_and_load_batch_data(
            project_id=project_id,
            bucket_name=bucket_name,
            dataset_name=dataset_name,
            target_files=target_files,
        )
        return True
    except Exception as e:
        print(f"Aviso/Error durante el proceso ETL Batch a BigQuery: {e}")
        print("Continuando con el flujo de streaming...")
        return False


def sync_fastf1_cache(
    bucket_name: str = GCS_BUCKET_NAME,
    source_folder: str = LOCAL_RAW_PATH,
) -> bool:
    """
    Sincroniza la caché de telemetría de FastF1 con Google Cloud Storage.
    Permite que instancias en la nube (GCP/AWS/etc.) ejecuten el replay sin ser
    bloqueadas con HTTP 403 por la CDN de Formula 1 (livetiming.formula1.com).
    """
    import tarfile

    archive_name = "fastf1_cache_2023.tar.gz"
    local_archive = os.path.join(source_folder, archive_name)
    gcs_blob_path = f"cache/{archive_name}"

    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Verificar si realmente existen archivos .ff1pkl en la carpeta local
        has_pkl_files = False
        for root, _, files in os.walk(source_folder):
            if any(f.endswith(".ff1pkl") for f in files):
                has_pkl_files = True
                break

        # Caso 1: Hay archivos .ff1pkl locales (entorno local) -> asegurar respaldo en GCS
        if has_pkl_files and not os.path.exists(local_archive):
            print("\nEmpaquetando caché local de FastF1 para respaldar en GCS Data Lake...")
            with tarfile.open(local_archive, "w:gz") as tar:
                year_dir = os.path.join(source_folder, "2023")
                if os.path.exists(year_dir):
                    tar.add(year_dir, arcname="2023")
                sqlite_file = os.path.join(source_folder, "fastf1_http_cache.sqlite")
                if os.path.exists(sqlite_file):
                    tar.add(sqlite_file, arcname="fastf1_http_cache.sqlite")

            print(f"Subiendo caché de FastF1 a gs://{bucket_name}/{gcs_blob_path}...")
            blob = bucket.blob(gcs_blob_path)
            blob.upload_from_filename(local_archive)
            print("✅ Caché de FastF1 respaldada en GCS exitosamente.")
            return True

        # Caso 2: Estamos en la nube (no hay archivos .ff1pkl locales) -> descargar de GCS
        elif not has_pkl_files:
            print("\n[!] Archivos .ff1pkl no encontrados localmente. Descargando desde GCS Data Lake...")
            blob = bucket.blob(gcs_blob_path)
            if blob.exists():
                print(f"Descargando gs://{bucket_name}/{gcs_blob_path} -> {local_archive}...")
                blob.download_to_filename(local_archive)
                print(f"Extrayendo archivos de telemetría en {source_folder}...")
                with tarfile.open(local_archive, "r:gz") as tar:
                    tar.extractall(path=source_folder)
                print("✅ Archivos .ff1pkl restaurados con éxito desde GCS. Las llamadas a la CDN serán omitidas.")
                return True
            else:
                print(f"Aviso: No se encontró {gcs_blob_path} en el bucket gs://{bucket_name}.")
                return False

        return True
    except Exception as e:
        print(f"Aviso en sincronización de caché FastF1: {e}")
        return False


def stream_all_drivers_telemetry(
    year: int = DEFAULT_REPLAY_YEAR,
    race: str = DEFAULT_REPLAY_RACE,
    session: str = DEFAULT_REPLAY_SESSION,
    delay: float = 0.2,
):
    """
    Paso 4: Transmite en tiempo real la telemetría de TODOS los pilotos de TODAS
    las escuderías (Ferrari, Red Bull, Mercedes, McLaren, etc.), intercalada
    cronológicamente para alimentar el pipeline en streaming (Pub/Sub -> BigQuery / Dashboard).
    """
    # Sincronizar caché de FastF1 con GCS antes de iniciar el streaming
    sync_fastf1_cache()

    print("\n" + "=" * 70)
    print(" PASO 4: STREAMING EN TIEMPO REAL - TODOS LOS PILOTOS Y ESCUDERÍAS")
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
    Orquestador principal del pipeline:
    1. Descarga datos batch históricos de Kaggle.
    2. Sube los archivos batch al Data Lake en Google Cloud Storage.
    3. Procesa y consolida las tablas batch en BigQuery (ETL).
    4. Inicia la transmisión en tiempo real de todos los pilotos y escuderías hacia Pub/Sub -> BigQuery.
    """
    print("*" * 70)
    print("       INICIANDO PIPELINE COMPLETO DE FORMULA 1 (BATCH + STREAMING)")
    print("*" * 70)

    # 1. Ingesta Batch: Kaggle -> Local
    download_kaggle_dataset()

    # 2. Ingesta Data Lake: Local -> GCS
    upload_to_gcs()

    # 3. ETL Data Warehouse: GCS -> BigQuery
    load_batch_to_bigquery()

    # 4. Streaming en tiempo real: Todos los pilotos -> Pub/Sub -> BigQuery
    stream_all_drivers_telemetry(delay=0.2)

    print("\n" + "*" * 70)
    print("       PIPELINE EJECUTADO CON ÉXITO")
    print("*" * 70)


if __name__ == "__main__":
    run_pipeline()
