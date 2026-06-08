import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "shop")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX      = os.getenv("KAFKA_TOPIC_PREFIX", "dbserver1.public")

# Debezium
DEBEZIUM_HOST = os.getenv("DEBEZIUM_HOST", "localhost")
DEBEZIUM_PORT = int(os.getenv("DEBEZIUM_PORT", 8083))

# Delta Lake paths
DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "./lakehouse")
BRONZE_PATH     = f"{DELTA_LAKE_PATH}/bronze"
SILVER_PATH     = f"{DELTA_LAKE_PATH}/silver"
GOLD_PATH       = f"{DELTA_LAKE_PATH}/gold"

# Spark Streaming checkpoints
_CHECKPOINT_BASE  = os.getenv("CHECKPOINT_BASE", "./checkpoints")
CHECKPOINT_BRONZE = f"{_CHECKPOINT_BASE}/bronze"
CHECKPOINT_SILVER = f"{_CHECKPOINT_BASE}/silver"
CHECKPOINT_GOLD   = f"{_CHECKPOINT_BASE}/gold"

# Spark
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "RealTimePlatform")
SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")
