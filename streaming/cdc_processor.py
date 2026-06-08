from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PREFIX
from schemas.cdc_schema import DEBEZIUM_ENVELOPE_SCHEMA


def read_kafka_stream(spark: SparkSession, table: str):
    topic = f"{KAFKA_TOPIC_PREFIX}.{table}"
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
    )


def parse_cdc_envelope(raw_df):
    """
    Kafka entrega el value como bytes.
    Debezium envía un JSON con {before, after, op, ts_ms, source}.
    """
    return (
        raw_df
        .select(
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast(StringType()), DEBEZIUM_ENVELOPE_SCHEMA).alias("cdc"),
        )
        .select(
            "offset",
            "partition",
            "kafka_timestamp",
            F.col("cdc.op").alias("op"),
            F.col("cdc.ts_ms").alias("event_ts_ms"),
            F.col("cdc.source.table").alias("source_table"),
            F.col("cdc.source.lsn").alias("lsn"),
            F.col("cdc.before").alias("before"),
            F.col("cdc.after").alias("after"),
        )
    )
