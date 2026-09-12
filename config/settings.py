import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file
load_dotenv(BASE_DIR / ".env")

# Google Cloud Platform configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
PUB_SUB_TOPIC = os.getenv("PUB_SUB_TOPIC", "f1-telemetry-topic")
PUB_SUB_SUBSCRIPTION = os.getenv("PUB_SUB_SUBSCRIPTION", "f1-telemetry-topic-sub")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "f1-batch-data-lake-ec")
GCS_DESTINATION_FOLDER = os.getenv("GCS_DESTINATION_FOLDER", "batch/")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "f1_insights")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "telemetry_realtime")
BIGQUERY_EMULATOR_HOST = os.getenv("BIGQUERY_EMULATOR_HOST", "")
if BIGQUERY_EMULATOR_HOST:
    os.environ["BIGQUERY_EMULATOR_HOST"] = BIGQUERY_EMULATOR_HOST

# Kaggle dataset configuration
KAGGLE_DATASET_NAME = os.getenv("KAGGLE_DATASET_NAME", "rohanrao/formula-1-world-championship-1950-2020")
LOCAL_RAW_PATH = os.getenv("LOCAL_RAW_PATH", str(BASE_DIR / "data" / "raw"))
TARGET_FILES = ["lap_times.csv", "drivers.csv", "races.csv", "circuits.csv"]

# Default Simulation / Replay Settings
DEFAULT_REPLAY_YEAR = 2023
DEFAULT_REPLAY_RACE = "Monza"
DEFAULT_REPLAY_SESSION = "R"

# Default driver roster across top teams (Constructors):
# Red Bull Racing: Verstappen (1), Perez (11)
# Ferrari: Leclerc (16), Sainz (55)
# Mercedes: Hamilton (44), Russell (63)
# McLaren: Norris (4), Piastri (81)
# Aston Martin: Alonso (14), Stroll (18)
DEFAULT_DRIVERS = ["1", "11", "16", "55", "44", "63", "4", "81", "14", "18"]
