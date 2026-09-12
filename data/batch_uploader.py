import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import (
    KAGGLE_DATASET_NAME,
    GCS_BUCKET_NAME,
    GCS_DESTINATION_FOLDER,
    LOCAL_RAW_PATH,
    TARGET_FILES,
)
from jobs.seed import download_kaggle_dataset, upload_to_gcs

if __name__ == "__main__":
    print("Ejecutando batch_uploader (Kaggle -> Local -> GCS)...")
    success = download_kaggle_dataset(
        dataset_name=KAGGLE_DATASET_NAME,
        download_path=LOCAL_RAW_PATH,
        target_files=TARGET_FILES,
    )
    if success:
        upload_to_gcs(
            bucket_name=GCS_BUCKET_NAME,
            source_folder=LOCAL_RAW_PATH,
            destination_folder=GCS_DESTINATION_FOLDER,
            target_files=TARGET_FILES,
        )