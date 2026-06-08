"""
Poller de trades reales de Binance → PostgreSQL.
Binance API pública, sin API key.

Endpoint: GET https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=100

Flujo: Binance REST → PostgreSQL (INSERT) → Debezium CDC → Kafka → Spark → Delta Lake

Ejecutar: python scripts/data_generator.py
"""

import time
import requests
import psycopg2
from datetime import datetime, timezone

from config.settings import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
)

BINANCE_API  = "https://api.binance.com/api/v3/trades"
SYMBOLS      = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TRADES_LIMIT = 5    # trades por símbolo por ciclo
POLL_SECONDS = 10   # intervalo entre ciclos


def connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


def fetch_trades(symbol: str) -> list[dict]:
    # /api/v3/trades devuelve siempre los últimos N trades (sin fromId)
    # La deduplicación la hace PostgreSQL con ON CONFLICT DO NOTHING
    try:
        resp = requests.get(BINANCE_API, params={"symbol": symbol, "limit": TRADES_LIMIT}, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [binance] Error {symbol}: {e}")
        return []


def insert_trades(cur, symbol: str, trades: list[dict]) -> int:
    inserted = 0
    for t in trades:
        try:
            cur.execute(
                """INSERT INTO trades
                       (trade_id, symbol, price, quantity, quote_qty, is_buyer_maker, trade_time)
                   VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0))
                   ON CONFLICT (trade_id) DO NOTHING""",
                (
                    t["id"],
                    symbol,
                    float(t["price"]),
                    float(t["qty"]),
                    float(t["quoteQty"]),
                    t["isBuyerMaker"],
                    t["time"],
                ),
            )
            if cur.rowcount:
                inserted += 1
        except Exception as e:
            print(f"  [db] Error insertando trade {t.get('id')}: {e}")
    return inserted


def run():
    conn = connect()
    conn.autocommit = False
    cur  = conn.cursor()

    total_inserted = 0
    cycle = 0

    print(f"[generator] Conectado a PostgreSQL — polling {len(SYMBOLS)} pares")
    print(f"[generator] Símbolos: {', '.join(SYMBOLS)}")
    print("[generator] Ctrl+C para parar\n")

    try:
        while True:
            cycle += 1
            cycle_inserted = 0
            print(f"── Ciclo {cycle} ({datetime.now(timezone.utc).strftime('%H:%M:%S')}) ──")

            for symbol in SYMBOLS:
                trades = fetch_trades(symbol)
                if not trades:
                    continue

                n = insert_trades(cur, symbol, trades)
                conn.commit()

                cycle_inserted += n
                last_price = float(trades[-1]["price"])
                print(f"  {symbol:10s}  +{n:3d} trades  último precio: {last_price:,.4f} USDT")

            total_inserted += cycle_inserted
            print(f"  total acumulado: {total_inserted} trades\n")
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print(f"\n[generator] Parado. {total_inserted} trades insertados en {cycle} ciclos.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
