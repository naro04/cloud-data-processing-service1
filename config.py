
import os
# ============================================
# DATABRICKS CONNECTION
# ============================================

# Databricks workspace URL
DATABRICKS_HOST = "https://dbc-957aa73b-d7e0.cloud.databricks.com"

# Access Token
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# ============================================
# DATASET CONFIGURATION
# ============================================

# Using Databricks built-in dataset
DATASET_PATH = "/databricks-datasets/flights/departuredelays.csv"

# ============================================
# APPLICATION SETTINGS
# ============================================

# Local folders
LOCAL_UPLOAD_FOLDER = "temp/uploads"
LOCAL_RESULTS_FOLDER = "temp/results"

# File settings
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500 MB
ALLOWED_EXTENSIONS = ['csv', 'json', 'txt', 'parquet']

# Worker configurations for testing
AVAILABLE_WORKERS = [1, 2, 4, 8]
DEFAULT_NUM_WORKERS = 1

# ============================================
# ML CONFIGURATION
# ============================================

FEATURE_COLUMNS = ['distance', 'delay']
TARGET_COLUMN = 'delay'

# ============================================
# HELPER FUNCTIONS
# ============================================

import os


def get_local_path(filename, folder='uploads'):

    if folder == 'uploads':
        os.makedirs(LOCAL_UPLOAD_FOLDER, exist_ok=True)
        return os.path.join(LOCAL_UPLOAD_FOLDER, filename)
    else:
        os.makedirs(LOCAL_RESULTS_FOLDER, exist_ok=True)
        return os.path.join(LOCAL_RESULTS_FOLDER, filename)


def validate_config():

    print("Validating configuration...")

    if not DATABRICKS_TOKEN or len(DATABRICKS_TOKEN) < 10:
        print(" Token is not set properly")
        return False
    else:
        print(f" Token is set: {DATABRICKS_TOKEN[:20]}...")

    if "xxxxx" in DATABRICKS_HOST or not DATABRICKS_HOST.startswith("https://"):
        print(" Workspace URL is not set properly")
        return False
    else:
        print(f" Workspace URL is set: {DATABRICKS_HOST}")

    print("\n Configuration is valid!")
    return True


def show_config():


    print("CLOUD DATA PROCESSING - CONFIGURATION")

    print(f"Platform: Databricks Free Edition")
    print(f"Workspace: {DATABRICKS_HOST}")
    print(f"Token: {DATABRICKS_TOKEN[:20]}...")
    print(f"Dataset: {DATASET_PATH}")
    print(f"Available Workers: {AVAILABLE_WORKERS}")



if __name__ == "__main__":
    show_config()
    print()
    validate_config()