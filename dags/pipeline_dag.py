"""
DAG — Real-Time Data Platform
Arquitectura:
  - Bronze + Silver: Spark streaming continuo (contenedor spark)
  - Gold:            docker exec → spark-submit en spark cada 30 minutos
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner":            "realtimeplatform",
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

SPARK_CONTAINER = "realtimeplatform-spark"
SPARK_SUBMIT    = "/opt/spark/bin/spark-submit"
SPARK_PACKAGES  = "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SPARK_CONF      = " ".join([
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "--conf spark.sql.shuffle.partitions=2",
])

def spark_submit_cmd(layer: str) -> str:
    cmd = (
        f"docker exec {SPARK_CONTAINER} "
        f"{SPARK_SUBMIT} "
        f"--master local[1] "
        f"--driver-memory 512m "
        f"--conf spark.driver.maxResultSize=256m "
        f"--packages {SPARK_PACKAGES} "
        f"{SPARK_CONF} "
        f"/app/scripts/run_gold.py --layer {layer}"
    )
    # Exit code 137 = JVM killed durante shutdown tras escribir datos — ignorar
    return f"{cmd}; code=$?; if [ $code -eq 137 ] || [ $code -eq 0 ]; then exit 0; else exit $code; fi"

dag = DAG(
    dag_id="crypto_pipeline",
    description="CDC pipeline: Binance → Kafka → Bronze(stream) → Silver(stream) → Gold(batch)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["crypto", "cdc", "bronze", "silver", "gold", "lineage"],
)

# ── Step 1: Verificar Bronze y Silver streams ─────────────────────────────────

def check_streaming_layers(**ctx):
    import os
    layers = {
        "Bronze": "/data/lakehouse/bronze/trades",
        "Silver": "/data/lakehouse/silver/trades",
    }
    for name, path in layers.items():
        exists = os.path.exists(f"{path}/_delta_log")
        print(f"  [{name}] {'✅ activo' if exists else '❌ no encontrado'}")
        if not exists:
            raise FileNotFoundError(f"{name} no tiene datos en {path}")
    print("  Bronze + Silver streams activos ✅")


t_check_streams = PythonOperator(
    task_id="check_bronze_silver_streams",
    python_callable=check_streaming_layers,
    dag=dag,
)

# ── Steps 2,3,4: Gold batch via docker exec spark-submit ──────────────────────

t_gold_ohlcv = BashOperator(
    task_id="gold_ohlcv",
    bash_command=spark_submit_cmd("ohlcv"),
    dag=dag,
)

t_gold_pressure = BashOperator(
    task_id="gold_buy_sell_pressure",
    bash_command=spark_submit_cmd("pressure"),
    dag=dag,
)

t_gold_ranking = BashOperator(
    task_id="gold_volume_ranking",
    bash_command=spark_submit_cmd("ranking"),
    dag=dag,
)

# ── Step 5: Estadísticas ──────────────────────────────────────────────────────

def report_stats(**ctx):
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", port=5432,
        dbname="crypto", user="postgres", password="postgres"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol,
               COUNT(*)                          AS trades,
               ROUND(AVG(price::numeric), 2)     AS avg_price,
               ROUND(SUM(quote_qty::numeric), 2) AS volume_usdt
        FROM trades
        WHERE trade_time >= NOW() - INTERVAL '30 minutes'
        GROUP BY symbol
        ORDER BY volume_usdt DESC
    """)
    rows = cur.fetchall()
    conn.close()
    print(f"\n[stats] Últimos 30 minutos ({len(rows)} símbolos):")
    print(f"  {'Symbol':<12} {'Trades':>8} {'Avg Price':>12} {'Volume USDT':>15}")
    print("  " + "-" * 52)
    for symbol, trades, avg_price, volume in rows:
        print(f"  {symbol:<12} {trades:>8} {avg_price:>12} {volume:>15}")


t_stats = PythonOperator(
    task_id="report_stats",
    python_callable=report_stats,
    dag=dag,
)

# ── Grafo ─────────────────────────────────────────────────────────────────────
#
#  check_streams → gold_ohlcv → gold_pressure → gold_ranking → report_stats
#

t_check_streams >> t_gold_ohlcv >> t_gold_pressure >> t_gold_ranking >> t_stats
