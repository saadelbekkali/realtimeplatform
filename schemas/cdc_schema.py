from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType, MapType
)

# Envelope que Debezium envuelve alrededor de cada evento CDC
DEBEZIUM_ENVELOPE_SCHEMA = StructType([
    StructField("before",       StringType(),    True),  # JSON del estado anterior
    StructField("after",        StringType(),    True),  # JSON del estado nuevo
    StructField("op",           StringType(),    False), # c=create, u=update, d=delete, r=read(snapshot)
    StructField("ts_ms",        LongType(),      True),  # timestamp del evento en ms
    StructField("source", StructType([
        StructField("db",       StringType(),    True),
        StructField("schema",   StringType(),    True),
        StructField("table",    StringType(),    True),
        StructField("ts_ms",    LongType(),      True),
        StructField("lsn",      LongType(),      True),  # Log Sequence Number de PostgreSQL
    ]),                                          True),
    StructField("transaction",  StringType(),    True),
])
