"""
Gold batch job — lanzado por Airflow via SparkSubmitOperator.
Uso: spark-submit run_gold.py --layer [ohlcv|pressure|ranking]
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from streaming.spark_session import get_spark
from lakehouse.gold_layer import write_gold_ohlcv, write_gold_pressure, write_gold_ranking


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=["ohlcv", "pressure", "ranking"])
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.layer == "ohlcv":
        print("[Gold] Ejecutando OHLCV batch...")
        write_gold_ohlcv(spark)

    elif args.layer == "pressure":
        print("[Gold] Ejecutando Buy/Sell Pressure batch...")
        write_gold_pressure(spark)

    elif args.layer == "ranking":
        print("[Gold] Ejecutando Volume Ranking batch...")
        write_gold_ranking(spark)

    print(f"[Gold] {args.layer} completado ✅")
    spark.stop()


if __name__ == "__main__":
    main()
