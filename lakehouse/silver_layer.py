from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.settings import BRONZE_PATH, SILVER_PATH, CHECKPOINT_SILVER
from schemas.orders_schema import TRADES_SCHEMA


def read_bronze_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("delta")
        .load(f"{BRONZE_PATH}/trades")
    )


def transform_silver(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("op").isin("c", "r"))   # solo inserts/snapshot, no updates
        .withColumn("data", F.from_json(F.col("after"), TRADES_SCHEMA))
        .select(
            F.col("lsn"),
            F.col("ingested_at"),
            F.current_timestamp().alias("processed_at"),
            F.col("data.trade_id"),
            F.col("data.symbol"),
            F.col("data.price"),
            F.col("data.quantity"),
            F.col("data.quote_qty"),
            F.col("data.is_buyer_maker"),
            F.to_timestamp(F.col("data.trade_time") / 1000).alias("trade_time"),
            F.when(F.col("data.is_buyer_maker"), F.lit("sell"))
             .otherwise(F.lit("buy")).alias("side"),
        )
        .dropDuplicates(["trade_id"])
    )


def write_silver(df: DataFrame):
    return (
        df
        .writeStream
        .queryName("silver_trades")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_SILVER}/trades")
        .option("path", f"{SILVER_PATH}/trades")
        .trigger(processingTime="60 seconds")
        .start()
    )
