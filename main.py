"""
Spark Structured Streaming — Bronze + Silver (streams continuos)
Gold se ejecuta como batch desde Airflow cada 30 minutos.
"""

import time
import os
from pyspark.sql.streaming import StreamingQueryListener

from streaming.spark_session import get_spark
from streaming.cdc_processor import read_kafka_stream, parse_cdc_envelope
from lakehouse.bronze_layer import write_bronze
from lakehouse.silver_layer import read_bronze_stream, transform_silver, write_silver
from config.settings import BRONZE_PATH, SILVER_PATH


class StreamingLogger(StreamingQueryListener):
    """Imprime el progreso de cada micro-batch."""
    def onQueryStarted(self, event):
        print(f"  [stream] arrancado: {event.name} ({event.id})")

    def onQueryProgress(self, event):
        p = event.progress
        rows = p.numInputRows
        rate = round(p.processedRowsPerSecond, 1) if p.processedRowsPerSecond else 0
        duration = p.durationMs.get("triggerExecution", 0)
        print(f"  [{p.name}] batch {p.batchId} — {rows} filas — {rate} rows/s — {duration}ms")

    def onQueryTerminated(self, event):
        print(f"  [stream] parado: {event.id}")


def wait_for_table(path: str, timeout: int = 120):
    print(f"  Esperando tabla Delta en {path} ...")
    start = time.time()
    while not os.path.exists(f"{path}/_delta_log"):
        if time.time() - start > timeout:
            raise TimeoutError(f"Tabla Delta no encontrada en {path} tras {timeout}s")
        time.sleep(5)
    print(f"  Tabla lista ✅")


def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(StreamingLogger())

    # ── Bronze: Kafka → Delta raw (stream continuo) ───────────────────────────
    print("\n[1/2] Arrancando Bronze (Kafka → Delta raw)...")
    raw_kafka = read_kafka_stream(spark, "trades")
    parsed    = parse_cdc_envelope(raw_kafka)
    bq        = write_bronze(parsed, "trades")
    wait_for_table(f"{BRONZE_PATH}/trades")

    # ── Silver: Bronze → trades limpios (stream continuo) ────────────────────
    print("\n[2/2] Arrancando Silver (Bronze → Delta limpio)...")
    bronze_df = read_bronze_stream(spark)
    silver_df = transform_silver(bronze_df)
    sq        = write_silver(silver_df)
    wait_for_table(f"{SILVER_PATH}/trades")

    print("\n Streaming activo — Bronze + Silver:")
    print(f"  Bronze stream : {bq.id}")
    print(f"  Silver stream : {sq.id}")
    print("\nGold se ejecuta como batch desde Airflow cada 30 minutos.")
    print("Ctrl+C para parar\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
