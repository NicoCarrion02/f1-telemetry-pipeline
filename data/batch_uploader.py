import os
import shutil
from dotenv import load_dotenv
import kagglehub
from google.cloud import storage

load_dotenv()

DATASET_NAME = "rohanrao/formula-1-world-championship-1950-2020"
BUCKET_NAME = "f1-batch-data-lake-ec"
GCS_DESTINATION_FOLDER = "batch/"
LOCAL_DOWNLOAD_PATH = "./data/raw"
TARGET_FILES = ["lap_times.csv", "drivers.csv", "races.csv", "circuits.csv"]

def download_kaggle_dataset():
    print("Iniciando descarga desde Kaggle...")
    ruta_cache = kagglehub.dataset_download(DATASET_NAME)

    os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)

    for file_name in TARGET_FILES:
        source = os.path.join(ruta_cache, file_name)
        destination = os.path.join(LOCAL_DOWNLOAD_PATH, file_name)

        if os.path.exists(source):
            shutil.copy(source, destination)
            print(f"Copiado: {file_name}")
        else:
            print(f"Advertencia: {file_name} no encontrado en la descarga.")

    print("Descarga y filtrado completados.")

def upload_to_gcs():
    print(f"Conectando a Google Cloud Storage (Bucket: {BUCKET_NAME})...")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for file_name in TARGET_FILES:
        local_file_path = os.path.join(LOCAL_DOWNLOAD_PATH, file_name)

        if os.path.exists(local_file_path):
            blob_path = f"{GCS_DESTINATION_FOLDER}{file_name}"
            blob = bucket.blob(blob_path)

            print(f"Subiendo {file_name} a gs://{BUCKET_NAME}/{blob_path} ...")
            blob.upload_from_filename(local_file_path)
        else:
            print(f"Advertencia: {file_name} no encontrado.")

    print("Carga a GCS finalizada exitosamente.")

if __name__ == "__main__":
    download_kaggle_dataset()
    upload_to_gcs()