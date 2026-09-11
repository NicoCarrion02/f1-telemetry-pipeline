import os
import sys
import json
import time
from dotenv import load_dotenv

if os.name == 'nt':
    base_dir = os.path.abspath("./hadoop_temp")
    bin_dir = os.path.join(base_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    winutils_path = os.path.join(bin_dir, "winutils.exe")
    if not os.path.exists(winutils_path):
        try:
            import urllib.request
            url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe"
            urllib.request.urlretrieve(url, winutils_path)
        except Exception:
            pass
    os.environ['HADOOP_HOME'] = base_dir

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import col
from google.cloud import pubsub_v1, bigquery

load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
SUBSCRIPTION_ID = "f1-telemetry-topic-sub"
TABLE_ID = f"{PROJECT_ID}.f1_insights.telemetry_realtime"

def get_spark_session():
    print("Iniciando Apache Spark...")
    spark = SparkSession.builder \
        .appName("F1_Stream_Batch_Processor") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream():
    spark = get_spark_session()
    bq_client = bigquery.Client()
    
    # 1. CARGA BATCH (Histórico manejando '\N' como nulo)
    print("Cargando dataset Batch de Kaggle (drivers.csv)...")
    batch_df = spark.read.option("nullValue", "\\N").csv("data/raw/drivers.csv", header=True, inferSchema=True)
    
    drivers_batch = batch_df.select(
        col("number").cast("int").alias("driver_number_batch"),
        col("forename").alias("nombre"),
        col("surname").alias("apellido"),
        col("nationality").alias("nacionalidad")
    ).dropna(subset=["driver_number_batch"])
    
    # 2. CONEXIÓN A PUB/SUB (Streaming)
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    print(f"Escuchando mensajes en {subscription_path}...")
    
    while True:
        try:
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 50},
                timeout=5.0
            )
            
            if not response.received_messages:
                time.sleep(2)
                continue
                
            messages_data = []
            ack_ids = []
            
            for msg in response.received_messages:
                try:
                    payload = json.loads(msg.message.data.decode("utf-8"))
                    messages_data.append(Row(**payload))
                    ack_ids.append(msg.ack_id)
                except Exception as e:
                    print(f"Error parseando mensaje: {e}")
            
            if messages_data:
                # 3. CREAR DATAFRAME Y HACER STREAM-BATCH JOIN
                stream_df = spark.createDataFrame(messages_data)
                
                enriched_df = stream_df.join(
                    drivers_batch,
                    stream_df.driver_number == drivers_batch.driver_number_batch,
                    "left"
                ).drop("driver_number_batch")
                
                # 4. CONVERTIR A PANDAS E INSERTAR EN BIGQUERY
                pdf = enriched_df.toPandas()
                if not pdf.empty:
                    print(f"Escribiendo lote de {len(pdf)} registros en BigQuery...")
                    errors = bq_client.insert_rows_from_dataframe(
                        bq_client.get_table(TABLE_ID), 
                        pdf
                    )
                    if errors == []:
                        print("Lote insertado exitosamente en BigQuery.\n")
                    else:
                        print(f"Errores al insertar en BigQuery: {errors}")
                
                subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
                
        except Exception as e:
            if "Deadline Exceeded" not in str(e):
                print(f"Esperando datos o error leve: {e}")
            time.sleep(2)

if __name__ == "__main__":
    process_stream()