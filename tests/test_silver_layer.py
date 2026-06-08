"""
Tests de la transformación Silver — parseo CDC, dedup, tipado.
"""

import json
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from lakehouse.silver_layer import transform_silver


def make_bronze_row(spark, trade_id, symbol, price, qty, is_buyer, op="c", lsn=1):
    """Crea un DataFrame simulando un evento Bronze (CDC parseado)."""
    after_data = {
        "trade_id":       trade_id,
        "symbol":         symbol,
        "price":          price,
        "quantity":       qty,
        "quote_qty":      round(price * qty, 8),
        "is_buyer_maker": is_buyer,
        "trade_time":     1717430400000,  # epoch ms
        "ingested_at":    None,
    }
    schema = StructType([
        StructField("op",           StringType(), True),
        StructField("event_ts_ms",  LongType(),   True),
        StructField("lsn",          LongType(),   True),
        StructField("ingested_at",  StringType(), True),
        StructField("after",        StringType(), True),
    ])
    data = [(op, 1717430400000, lsn, "2024-01-01", json.dumps(after_data))]
    return spark.createDataFrame(data, schema)


class TestSilverTransform:

    def test_parsea_campo_after(self, spark):
        df = make_bronze_row(spark, 1001, "BTCUSDT", 50000.0, 0.01, False)
        result = transform_silver(df)
        assert result.count() == 1
        row = result.first()
        assert row["symbol"] == "BTCUSDT"
        assert row["price"] == 50000.0

    def test_asigna_side_buy(self, spark):
        """is_buyer_maker=False → side=buy"""
        df = make_bronze_row(spark, 1002, "ETHUSDT", 3000.0, 0.5, False)
        result = transform_silver(df)
        assert result.first()["side"] == "buy"

    def test_asigna_side_sell(self, spark):
        """is_buyer_maker=True → side=sell"""
        df = make_bronze_row(spark, 1003, "ETHUSDT", 3000.0, 0.5, True)
        result = transform_silver(df)
        assert result.first()["side"] == "sell"

    def test_filtra_operacion_delete(self, spark):
        """op='d' debe ser ignorado en Silver."""
        df = make_bronze_row(spark, 1004, "BTCUSDT", 50000.0, 0.01, False, op="d")
        result = transform_silver(df)
        assert result.count() == 0

    def test_filtra_operacion_update(self, spark):
        """op='u' debe ignorarse — Silver solo acepta creates ('c') y snapshots ('r')."""
        df = make_bronze_row(spark, 1005, "BTCUSDT", 50000.0, 0.01, False, op="u")
        result = transform_silver(df)
        assert result.count() == 0

    def test_deduplica_por_trade_id(self, spark):
        """Dos filas con el mismo trade_id → solo una en Silver."""
        df1 = make_bronze_row(spark, 1006, "BTCUSDT", 50000.0, 0.01, False, lsn=10)
        df2 = make_bronze_row(spark, 1006, "BTCUSDT", 50001.0, 0.01, False, lsn=11)
        df = df1.union(df2)
        result = transform_silver(df)
        assert result.count() == 1

    def test_convierte_trade_time_a_timestamp(self, spark):
        df = make_bronze_row(spark, 1007, "SOLUSDT", 150.0, 10.0, True)
        result = transform_silver(df)
        row = result.first()
        assert row["trade_time"] is not None

    def test_calcula_quote_qty(self, spark):
        df = make_bronze_row(spark, 1008, "BNBUSDT", 400.0, 2.0, False)
        result = transform_silver(df)
        row = result.first()
        assert row["quote_qty"] == pytest.approx(800.0, rel=1e-4)
