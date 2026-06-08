from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType
)

TRADES_SCHEMA = StructType([
    StructField("trade_id",       LongType(),     False),
    StructField("symbol",         StringType(),   True),
    StructField("price",          DoubleType(),   True),
    StructField("quantity",       DoubleType(),   True),
    StructField("quote_qty",      DoubleType(),   True),   # valor en USDT
    StructField("is_buyer_maker", BooleanType(),  True),   # True=venta, False=compra
    StructField("trade_time",     LongType(),     True),   # epoch ms
])
