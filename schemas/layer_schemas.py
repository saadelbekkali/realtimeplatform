from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType,
    TimestampType, IntegerType,
)

# ── Bronze ─────────────────────────────────────────────────────────────────────
# Eventos CDC crudos tal como llegan de Kafka/Debezium.
# Escritura: lakehouse/bronze/trades (Delta, append)

BRONZE_SCHEMA = StructType([
    StructField("offset",          LongType(),      True),
    StructField("partition",       IntegerType(),   True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("op",              StringType(),    True),  # c=create r=snapshot u=update d=delete
    StructField("event_ts_ms",     LongType(),      True),
    StructField("source_table",    StringType(),    True),
    StructField("lsn",             LongType(),      True),
    StructField("before",          StringType(),    True),  # JSON estado anterior
    StructField("after",           StringType(),    True),  # JSON estado nuevo
    StructField("ingested_at",     TimestampType(), True),
])

# ── Silver ─────────────────────────────────────────────────────────────────────
# Trades limpios, tipados y deduplicados.
# Escritura: lakehouse/silver/trades (Delta, append)

SILVER_SCHEMA = StructType([
    StructField("lsn",             LongType(),      True),
    StructField("ingested_at",     TimestampType(), True),
    StructField("processed_at",    TimestampType(), True),
    StructField("trade_id",        LongType(),      False),
    StructField("symbol",          StringType(),    True),
    StructField("price",           DoubleType(),    True),
    StructField("quantity",        DoubleType(),    True),
    StructField("quote_qty",       DoubleType(),    True),
    StructField("is_buyer_maker",  BooleanType(),   True),
    StructField("trade_time",      TimestampType(), True),
    StructField("side",            StringType(),    True),  # buy | sell
])

# ── Gold: OHLCV ────────────────────────────────────────────────────────────────
# Velas de 1 minuto (todo el histórico).
# Escritura: lakehouse/gold/ohlcv (Delta, overwrite cada 30 min)

GOLD_OHLCV_SCHEMA = StructType([
    StructField("candle_time",  TimestampType(), True),
    StructField("symbol",       StringType(),    True),
    StructField("open",         DoubleType(),    True),
    StructField("high",         DoubleType(),    True),
    StructField("low",          DoubleType(),    True),
    StructField("close",        DoubleType(),    True),
    StructField("volume_base",  DoubleType(),    True),
    StructField("volume_usdt",  DoubleType(),    True),
    StructField("num_trades",   LongType(),      True),
    StructField("computed_at",  TimestampType(), True),
])

# ── Gold: Buy/Sell Pressure ────────────────────────────────────────────────────
# Presión compradora vs vendedora por minuto, últimas 24h.
# Escritura: lakehouse/gold/buy_sell_pressure (Delta, overwrite cada 30 min)

GOLD_PRESSURE_SCHEMA = StructType([
    StructField("window_time",  TimestampType(), True),
    StructField("symbol",       StringType(),    True),
    StructField("side",         StringType(),    True),  # buy | sell
    StructField("volume_usdt",  DoubleType(),    True),
    StructField("num_trades",   LongType(),      True),
    StructField("avg_price",    DoubleType(),    True),
    StructField("computed_at",  TimestampType(), True),
])

# ── Gold: Volume Ranking ───────────────────────────────────────────────────────
# Ranking de símbolos por volumen USDT, últimas 24h.
# Escritura: lakehouse/gold/volume_ranking (Delta, overwrite cada 30 min)

GOLD_RANKING_SCHEMA = StructType([
    StructField("symbol",             StringType(), True),
    StructField("total_volume_usdt",  DoubleType(), True),
    StructField("total_trades",       LongType(),   True),
    StructField("price_low",          DoubleType(), True),
    StructField("price_high",         DoubleType(), True),
    StructField("last_price",         DoubleType(), True),
    StructField("price_range_pct",    DoubleType(), True),
    StructField("computed_at",        TimestampType(), True),
])
