"""
Tests de la capa Gold — agregaciones OHLCV, presión y ranking.
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, LongType, TimestampType
)


def make_silver_df(spark, rows):
    """
    rows: lista de (trade_id, symbol, price, quantity, quote_qty, is_buyer_maker, side, trade_time)
    """
    schema = StructType([
        StructField("trade_id",       LongType(),      False),
        StructField("symbol",         StringType(),    True),
        StructField("price",          DoubleType(),    True),
        StructField("quantity",       DoubleType(),    True),
        StructField("quote_qty",      DoubleType(),    True),
        StructField("is_buyer_maker", BooleanType(),   True),
        StructField("side",           StringType(),    True),
        StructField("trade_time",     TimestampType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def ts(minute: int):
    """Helper — timestamp en minuto X del día."""
    return datetime(2024, 6, 3, 12, minute, 0, tzinfo=timezone.utc)


class TestGoldOHLCV:

    def test_ohlcv_calcula_high_y_low(self, spark):
        rows = [
            (1, "BTCUSDT", 50000.0, 0.1, 5000.0, False, "buy",  ts(0)),
            (2, "BTCUSDT", 51000.0, 0.1, 5100.0, True,  "sell", ts(0)),
            (3, "BTCUSDT", 49500.0, 0.1, 4950.0, False, "buy",  ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy(F.date_trunc("minute", "trade_time").alias("candle_time"), "symbol")
              .agg(F.max("price").alias("high"), F.min("price").alias("low"))
        )
        row = result.first()
        assert row["high"] == 51000.0
        assert row["low"]  == 49500.0

    def test_ohlcv_suma_volumen(self, spark):
        rows = [
            (1, "ETHUSDT", 3000.0, 1.0, 3000.0, False, "buy",  ts(1)),
            (2, "ETHUSDT", 3100.0, 2.0, 6200.0, True,  "sell", ts(1)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy(F.date_trunc("minute", "trade_time").alias("candle_time"), "symbol")
              .agg(F.sum("quote_qty").alias("volume_usdt"))
        )
        assert result.first()["volume_usdt"] == pytest.approx(9200.0)

    def test_ohlcv_separa_por_simbolo(self, spark):
        rows = [
            (1, "BTCUSDT", 50000.0, 0.1, 5000.0, False, "buy",  ts(0)),
            (2, "ETHUSDT",  3000.0, 1.0, 3000.0, False, "buy",  ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy(F.date_trunc("minute", "trade_time").alias("candle_time"), "symbol")
              .agg(F.count("trade_id").alias("trades"))
        )
        assert result.count() == 2


class TestGoldPressure:

    def test_separa_buy_y_sell(self, spark):
        rows = [
            (1, "BTCUSDT", 50000.0, 0.1, 5000.0, False, "buy",  ts(0)),
            (2, "BTCUSDT", 50100.0, 0.2, 10020.0, True, "sell", ts(0)),
            (3, "BTCUSDT", 49900.0, 0.1, 4990.0, False, "buy",  ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy("symbol", "side")
              .agg(F.count("trade_id").alias("num_trades"))
        )
        buy_row  = result.filter(F.col("side") == "buy").first()
        sell_row = result.filter(F.col("side") == "sell").first()
        assert buy_row["num_trades"]  == 2
        assert sell_row["num_trades"] == 1

    def test_volumen_comprador_mayor(self, spark):
        rows = [
            (1, "BTCUSDT", 50000.0, 1.0, 50000.0, False, "buy",  ts(0)),
            (2, "BTCUSDT", 50000.0, 1.0, 50000.0, False, "buy",  ts(0)),
            (3, "BTCUSDT", 50000.0, 0.5, 25000.0, True,  "sell", ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy("symbol", "side")
              .agg(F.sum("quote_qty").alias("volume_usdt"))
        )
        buy_vol  = result.filter(F.col("side") == "buy").first()["volume_usdt"]
        sell_vol = result.filter(F.col("side") == "sell").first()["volume_usdt"]
        assert buy_vol > sell_vol


class TestGoldRanking:

    def test_ranking_ordena_por_volumen(self, spark):
        rows = [
            (1, "BTCUSDT", 50000.0, 1.0, 50000.0, False, "buy",  ts(0)),
            (2, "ETHUSDT",  3000.0, 1.0,  3000.0, False, "buy",  ts(0)),
            (3, "SOLUSDT",   150.0, 1.0,   150.0, False, "buy",  ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy("symbol")
              .agg(F.sum("quote_qty").alias("total_volume_usdt"))
              .orderBy(F.col("total_volume_usdt").desc())
        )
        symbols = [r["symbol"] for r in result.collect()]
        assert symbols[0] == "BTCUSDT"
        assert symbols[-1] == "SOLUSDT"

    def test_ranking_calcula_rango_precio(self, spark):
        rows = [
            (1, "BTCUSDT", 48000.0, 0.1, 4800.0, False, "buy",  ts(0)),
            (2, "BTCUSDT", 52000.0, 0.1, 5200.0, True,  "sell", ts(0)),
        ]
        df = make_silver_df(spark, rows)
        result = (
            df.groupBy("symbol")
              .agg(
                  ((F.max("price") - F.min("price")) / F.min("price") * 100)
                  .alias("price_range_pct")
              )
        )
        pct = result.first()["price_range_pct"]
        assert pct == pytest.approx(8.333, rel=1e-2)
