from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config.settings import BRONZE_PATH, CHECKPOINT_BRONZE


def write_bronze(parsed_df: DataFrame, table: str):
    """
    Escribe el evento CDC crudo en Delta Lake Bronze.
    Sin transformaciones — todo el envelope Debezium se persiste tal cual.
    """
    path       = f"{BRONZE_PATH}/{table}"
    checkpoint = f"{CHECKPOINT_BRONZE}/{table}"

    return (
        parsed_df
        .withColumn("ingested_at", F.current_timestamp())
        .writeStream
        .queryName(f"bronze_{table}")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("path", path)
        .trigger(processingTime="60 seconds")
        .start()
    )
