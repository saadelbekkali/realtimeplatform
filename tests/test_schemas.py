"""
Tests de schemas — verifican que los StructType están bien definidos.
"""

import pytest
from pyspark.sql.types import (
    StructType, LongType, StringType, DoubleType, BooleanType
)
from schemas.cdc_schema import DEBEZIUM_ENVELOPE_SCHEMA
from schemas.orders_schema import TRADES_SCHEMA


class TestDebeziumEnvelopeSchema:

    def test_tiene_campo_op(self):
        campos = [f.name for f in DEBEZIUM_ENVELOPE_SCHEMA.fields]
        assert "op" in campos

    def test_tiene_campo_before_y_after(self):
        campos = [f.name for f in DEBEZIUM_ENVELOPE_SCHEMA.fields]
        assert "before" in campos
        assert "after" in campos

    def test_tiene_campo_ts_ms(self):
        campos = [f.name for f in DEBEZIUM_ENVELOPE_SCHEMA.fields]
        assert "ts_ms" in campos

    def test_tiene_campo_source(self):
        campos = [f.name for f in DEBEZIUM_ENVELOPE_SCHEMA.fields]
        assert "source" in campos

    def test_source_tiene_lsn(self):
        source_field = next(f for f in DEBEZIUM_ENVELOPE_SCHEMA.fields if f.name == "source")
        source_campos = [f.name for f in source_field.dataType.fields]
        assert "lsn" in source_campos

    def test_source_tiene_tabla(self):
        source_field = next(f for f in DEBEZIUM_ENVELOPE_SCHEMA.fields if f.name == "source")
        source_campos = [f.name for f in source_field.dataType.fields]
        assert "table" in source_campos


class TestTradesSchema:

    def test_tiene_todos_los_campos(self):
        campos = [f.name for f in TRADES_SCHEMA.fields]
        esperados = ["trade_id", "symbol", "price", "quantity",
                     "quote_qty", "is_buyer_maker", "trade_time"]
        for campo in esperados:
            assert campo in campos, f"Falta campo: {campo}"

    def test_trade_id_es_long(self):
        field = next(f for f in TRADES_SCHEMA.fields if f.name == "trade_id")
        assert isinstance(field.dataType, LongType)

    def test_price_es_double(self):
        field = next(f for f in TRADES_SCHEMA.fields if f.name == "price")
        assert isinstance(field.dataType, DoubleType)

    def test_is_buyer_maker_es_boolean(self):
        field = next(f for f in TRADES_SCHEMA.fields if f.name == "is_buyer_maker")
        assert isinstance(field.dataType, BooleanType)

    def test_symbol_es_string(self):
        field = next(f for f in TRADES_SCHEMA.fields if f.name == "symbol")
        assert isinstance(field.dataType, StringType)
