"""
Gold Layer — batch job ejecutado por Airflow cada 30 minutos.
Lee Silver (Delta) y escribe agregaciones en Gold (Delta).

Ventanas:
  - OHLCV    → todo el histórico (las velas son inmutables)
  - Pressure → últimas 24h (ventana deslizante)
  - Ranking  → últimas 24h (ventana deslizante)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.settings import SILVER_PATH, GOLD_PATH


def read_silver(spark: SparkSession) -> DataFrame:
    """Lee todo el histórico de Silver — usado por OHLCV."""
    return spark.read.format("delta").load(f"{SILVER_PATH}/trades")


def read_silver_24h(spark: SparkSession) -> DataFrame:
    """Lee solo las últimas 24h de Silver — ventana deslizante."""
    return (
        spark.read.format("delta").load(f"{SILVER_PATH}/trades")
        .filter(F.col("trade_time") >= F.now() - F.expr("INTERVAL 24 HOURS"))
    )


# ── OHLCV por minuto ──────────────────────────────────────────────────────────

def write_gold_ohlcv(spark: SparkSession):
    df = read_silver(spark)

    ohlcv = (
        df
        .groupBy(
            F.date_trunc("minute", F.col("trade_time")).alias("candle_time"),
            "symbol"
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume_base"),
            F.sum("quote_qty").alias("volume_usdt"),
            F.count("trade_id").alias("num_trades"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    (
        ohlcv.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_PATH}/ohlcv")
    )
    print(f"    OHLCV: {ohlcv.count()} velas escritas")


# ── Presión compradora vs vendedora ───────────────────────────────────────────

def write_gold_pressure(spark: SparkSession):
    df = read_silver_24h(spark)

    pressure = (
        df
        .groupBy(
            F.date_trunc("minute", F.col("trade_time")).alias("window_time"),
            "symbol",
            "side"
        )
        .agg(
            F.sum("quote_qty").alias("volume_usdt"),
            F.count("trade_id").alias("num_trades"),
            F.avg("price").alias("avg_price"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    (
        pressure.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_PATH}/buy_sell_pressure")
    )
    print(f"    Pressure: {pressure.count()} filas escritas")


# ── Ranking de volumen ────────────────────────────────────────────────────────

def write_gold_ranking(spark: SparkSession):
    df = read_silver_24h(spark)

    ranking = (
        df
        .groupBy("symbol")
        .agg(
            F.sum("quote_qty").alias("total_volume_usdt"),
            F.count("trade_id").alias("total_trades"),
            F.min("price").alias("price_low"),
            F.max("price").alias("price_high"),
            F.last("price").alias("last_price"),
            (
                (F.max("price") - F.min("price")) / F.min("price") * 100
            ).alias("price_range_pct"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.col("total_volume_usdt").desc())
    )

    (
        ranking.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_PATH}/volume_ranking")
    )
    print(f"    Ranking: {ranking.count()} símbolos escritos")
