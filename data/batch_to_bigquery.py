import os
import io
import sys
import json
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
from google.cloud import bigquery, storage
from dotenv import load_dotenv

from config.settings import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_DESTINATION_FOLDER,
    LOCAL_RAW_PATH,
    BIGQUERY_DATASET,
    TARGET_FILES,
)

load_dotenv()
PROJECT_ID = GCP_PROJECT_ID or os.getenv("GCP_PROJECT_ID", "")
BUCKET_NAME = GCS_BUCKET_NAME or os.getenv("GCS_BUCKET_NAME", "f1-batch-data-lake-ec")
DATASET_NAME = BIGQUERY_DATASET or os.getenv("BIGQUERY_DATASET", "f1_insights")


def load_csv_from_gcs_or_local(bucket_name: str, blob_path: str, local_fallback_path: str) -> pd.DataFrame:
    """Lee el archivo CSV desde GCS mediante google-cloud-storage; si no está disponible, lee de la copia local."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if blob.exists():
            csv_data = blob.download_as_text(encoding="utf-8")
            return pd.read_csv(io.StringIO(csv_data), na_values=['\\N'])
    except Exception as e:
        print(f"Aviso al leer de GCS gs://{bucket_name}/{blob_path}: {e}")

    # Fallback al archivo local descargado
    if os.path.exists(local_fallback_path):
        print(f"Usando copia local de respaldo: {local_fallback_path}")
        return pd.read_csv(local_fallback_path, na_values=['\\N'])

    raise FileNotFoundError(f"No se pudo encontrar el archivo ni en gs://{bucket_name}/{blob_path} ni en {local_fallback_path}")


def process_and_load_batch_data(
    project_id: str = PROJECT_ID,
    bucket_name: str = BUCKET_NAME,
    dataset_name: str = DATASET_NAME,
    target_files: list = TARGET_FILES,
):
    print("\n" + "=" * 70)
    print(" INICIANDO PROCESO ETL BATCH: GCS -> BIGQUERY")
    print(f" Proyecto: {project_id} | Dataset: {dataset_name} | Bucket: gs://{bucket_name}")
    print("=" * 70)

    bq_client = bigquery.Client()
    archivos_batch = target_files or ["drivers.csv", "races.csv", "circuits.csv", "lap_times.csv"]

    for file_name in archivos_batch:
        table_name = file_name.replace(".csv", "")
        blob_path = f"{GCS_DESTINATION_FOLDER}{file_name}"
        local_path = os.path.join(LOCAL_RAW_PATH, file_name)

        print(f"\nExtrayendo y limpiando: {file_name}...")

        # 1. EXTRACCIÓN Y LIMPIEZA DE NULOS
        df = load_csv_from_gcs_or_local(bucket_name, blob_path, local_path)

        # 2. TRANSFORMACIÓN Y CASTEO DE TIPOS DE DATOS
        if table_name == "drivers":
            df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
            df['number'] = pd.to_numeric(df['number'], errors='coerce').astype('Int64')

        elif table_name == "races":
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        elif table_name == "circuits":
            df['alt'] = pd.to_numeric(df['alt'], errors='coerce').astype('Int64')

        elif table_name == "lap_times":
            df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce').astype('Int64')

        # 3. CARGA A BIGQUERY
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        try:
            print(f"Cargando {len(df)} registros en {table_id}...")
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
            job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            print(f"✅ Tabla '{table_name}' consolidada exitosamente en BigQuery.")
        except Exception as e:
            if "unspecified job configuration query" in str(e) or os.getenv("BIGQUERY_EMULATOR_HOST"):
                print(f"Aviso: El emulador de BigQuery no soporta multipart load_table_from_dataframe.")
                print(f"Insertando registros en el emulador vía insert_rows_json...")
                try:
                    schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
                    table = bigquery.Table(table_id, schema=schema)
                    try:
                        bq_client.create_table(table, exists_ok=True)
                    except Exception:
                        pass
                    # Tomar lote de registros para el emulador (hasta 5000 por tabla para agilidad)
                    sample = df.head(5000).fillna("")
                    records = json.loads(sample.to_json(orient="records", date_format="iso"))
                    errors = bq_client.insert_rows_json(table_id, records)
                    if not errors:
                        print(f"✅ Tabla '{table_name}' creada e insertada en el emulador ({len(records)} filas).")
                    else:
                        print(f"Aviso al insertar en emulador: {errors}")
                except Exception as emu_err:
                    print(f"Aviso cargando en emulador: {emu_err}")
            else:
                print(f"Error cargando tabla {table_name}: {e}")


if __name__ == "__main__":
    process_and_load_batch_data()