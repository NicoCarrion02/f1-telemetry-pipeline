import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = "f1-batch-data-lake-ec"

def process_and_load_batch_data():
    print("Iniciando proceso ETL: GCS -> BigQuery")
    bq_client = bigquery.Client()
    
    archivos_batch = ["drivers.csv", "races.csv", "circuits.csv", "lap_times.csv"]
    
    for file_name in archivos_batch:
        table_name = file_name.replace(".csv", "")
        gcs_uri = f"gs://{BUCKET_NAME}/batch/{file_name}"
        
        print(f"\nExtraendo y limpiando: {file_name}...")
        
        # 1. EXTRACCIÓN Y LIMPIEZA DE NULOS
        df = pd.read_csv(gcs_uri, na_values=['\\N'])
        
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
        table_id = f"{PROJECT_ID}.f1_insights.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        print(f"Cargando {len(df)} registros en {table_id}...")
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        print(f"✅ Tabla '{table_name}' consolidada exitosamente en BigQuery.")

if __name__ == "__main__":
    process_and_load_batch_data()