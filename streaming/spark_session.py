from pyspark.sql import SparkSession
from config.settings import SPARK_APP_NAME, SPARK_MASTER

DELTA_MAVEN   = "io.delta:delta-spark_2.12:3.1.0"
KAFKA_MAVEN   = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def get_spark() -> SparkSession:
    # Si ya existe una sesión activa (lanzada por spark-submit) la reutilizamos
    existing = SparkSession.getActiveSession()
    if existing:
        return existing

    # Modo local (desarrollo o tests) — descarga los jars
    return (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.jars.packages", f"{DELTA_MAVEN},{KAFKA_MAVEN}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )
