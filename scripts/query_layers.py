"""
Consulta las tablas Delta Lake de cada capa del pipeline.
Ejecutar: python -m scripts.query_layers
"""

from streaming.spark_session import get_spark
from config.settings import BRONZE_PATH, SILVER_PATH, GOLD_PATH


def query_layer(spark, name: str, path: str, limit: int = 10):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    try:
        df = spark.read.format("delta").load(path)
        print(f"  Total filas: {df.count()}")
        print(f"  Schema:")
        df.printSchema()
        print(f"  Últimas {limit} filas:")
        df.orderBy("ingested_at", ascending=False).show(limit, truncate=False)
    except Exception as e:
        print(f"  ⚠️  Tabla no disponible aún: {e}")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    query_layer(spark, "BRONZE — Eventos CDC crudos",
                f"{BRONZE_PATH}/trades")

    query_layer(spark, "SILVER — Trades limpios y tipados",
                f"{SILVER_PATH}/trades")

    query_layer(spark, "GOLD — OHLCV (velas 1 minuto)",
                f"{GOLD_PATH}/ohlcv")

    query_layer(spark, "GOLD — Presión compradora/vendedora",
                f"{GOLD_PATH}/buy_sell_pressure")

    query_layer(spark, "GOLD — Ranking de volumen",
                f"{GOLD_PATH}/volume_ranking")

    spark.stop()


if __name__ == "__main__":
    main()
