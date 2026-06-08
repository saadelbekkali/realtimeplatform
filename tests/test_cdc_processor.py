"""
Tests del procesador CDC — parseo del envelope Debezium.
"""

import json
import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, BinaryType
from streaming.cdc_processor import parse_cdc_envelope


def make_kafka_df(spark, value_dict, offset=0):
    """Simula un mensaje Kafka con el envelope Debezium como bytes."""
    schema = StructType([
        StructField("offset",          LongType(),   True),
        StructField("partition",       LongType(),   True),
        StructField("timestamp",       LongType(),   True),
        StructField("value",           BinaryType(), True),
    ])
    value_bytes = json.dumps(value_dict).encode("utf-8")
    data = [(offset, 0, 1717430400000, value_bytes)]
    return spark.createDataFrame(data, schema)


def debezium_event(op="c", table="trades", trade_id=1, price=50000.0):
    after = json.dumps({
        "trade_id": trade_id, "symbol": "BTCUSDT",
        "price": price, "quantity": 0.01,
        "quote_qty": price * 0.01, "is_buyer_maker": False,
        "trade_time": 1717430400000, "ingested_at": None
    })
    return {
        "op":     op,
        "ts_ms":  1717430400000,
        "before": None,
        "after":  after,
        "source": {
            "db": "crypto", "schema": "public",
            "table": table, "ts_ms": 1717430400000, "lsn": 12345
        },
        "transaction": None
    }


class TestCDCProcessor:

    def test_parsea_campo_op(self, spark):
        df = make_kafka_df(spark, debezium_event(op="c"))
        result = parse_cdc_envelope(df)
        assert result.first()["op"] == "c"

    def test_parsea_campo_lsn(self, spark):
        df = make_kafka_df(spark, debezium_event())
        result = parse_cdc_envelope(df)
        assert result.first()["lsn"] == 12345

    def test_parsea_source_table(self, spark):
        df = make_kafka_df(spark, debezium_event(table="trades"))
        result = parse_cdc_envelope(df)
        assert result.first()["source_table"] == "trades"

    def test_parsea_after_como_string(self, spark):
        df = make_kafka_df(spark, debezium_event())
        result = parse_cdc_envelope(df)
        after = result.first()["after"]
        assert after is not None
        parsed = json.loads(after)
        assert parsed["symbol"] == "BTCUSDT"

    def test_mantiene_kafka_timestamp(self, spark):
        df = make_kafka_df(spark, debezium_event())
        result = parse_cdc_envelope(df)
        assert result.first()["kafka_timestamp"] is not None

    def test_evento_delete_tiene_op_d(self, spark):
        df = make_kafka_df(spark, debezium_event(op="d"))
        result = parse_cdc_envelope(df)
        assert result.first()["op"] == "d"

    def test_evento_update_tiene_op_u(self, spark):
        df = make_kafka_df(spark, debezium_event(op="u"))
        result = parse_cdc_envelope(df)
        assert result.first()["op"] == "u"
