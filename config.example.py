import os

# PostgreSQL — copy this file to config.py and fill in your values
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB   = os.getenv("PG_DB",   "nasa_etl")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "your_password_here")

# MongoDB
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",  "nasa_etl")
MONGO_COLLECTION = "nasa_logs"

# Hadoop / Hive / Pig paths (adjust for your installation)
HADOOP_HOME = os.getenv("HADOOP_HOME", "/usr/local/hadoop")
HIVE_HOST   = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT   = int(os.getenv("HIVE_PORT", 10000))
HIVE_DB     = os.getenv("HIVE_DB", "nasa_etl")
PIG_HOME    = os.getenv("PIG_HOME", "/usr/local/pig")

# Data files
import os as _os
DATA_DIR = _os.path.join(_os.path.dirname(__file__), "data")
LOG_FILES = [
    _os.path.join(DATA_DIR, "NASA_access_log_Jul95"),
    _os.path.join(DATA_DIR, "NASA_access_log_Aug95"),
]

DEFAULT_BATCH_SIZE = 50_000
