#!/bin/bash
# Estado del pipeline en tiempo real

echo "════════════════════════════════════════"
echo "   REAL-TIME DATA PLATFORM — STATUS"
echo "════════════════════════════════════════"

# ── Contenedores ─────────────────────────────────────────────────────────────
echo ""
echo "── Contenedores ──"
for name in postgres zookeeper kafka debezium generator spark; do
    container="realtimeplatform-$name"
    status=$(docker inspect --format='{{.State.Status}}' "$container" 2>/dev/null)
    health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container" 2>/dev/null)
    if [ "$status" = "running" ]; then
        if [ -n "$health" ]; then
            echo "  ✅ $name ($health)"
        else
            echo "  ✅ $name"
        fi
    else
        echo "  ❌ $name ($status)"
    fi
done

# ── Kafka: mensajes en el topic ───────────────────────────────────────────────
echo ""
echo "── Kafka ──"
msg_count=$(docker exec realtimeplatform-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic dbserver1.public.trades \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')
if [ -n "$msg_count" ] && [ "$msg_count" -gt 0 ]; then
    echo "  ✅ Topic dbserver1.public.trades — $msg_count mensajes"
else
    echo "  ❌ Topic dbserver1.public.trades — sin mensajes o no existe"
fi

# ── Debezium: estado del conector ────────────────────────────────────────────
echo ""
echo "── Debezium ──"
connector_state=$(curl -s http://localhost:8083/connectors/postgres-connector/status 2>/dev/null | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
if [ "$connector_state" = "RUNNING" ]; then
    echo "  ✅ postgres-connector RUNNING"
else
    echo "  ❌ postgres-connector $connector_state"
fi

# ── Delta Lake: capas ────────────────────────────────────────────────────────
echo ""
echo "── Delta Lake ──"
for layer in bronze/trades silver/trades gold/ohlcv gold/buy_sell_pressure gold/volume_ranking; do
    exists=$(docker exec realtimeplatform-spark test -d "/data/lakehouse/$layer/_delta_log" 2>/dev/null; echo $?)
    if [ "$exists" = "0" ]; then
        echo "  ✅ $layer"
    else
        echo "  ❌ $layer (aún no creado)"
    fi
done

# ── Spark Streaming: activo ───────────────────────────────────────────────────
echo ""
echo "── Spark Streaming ──"
bronze_ok=$(docker exec realtimeplatform-spark test -d "/data/lakehouse/bronze/trades/_delta_log" 2>/dev/null; echo $?)
silver_ok=$(docker exec realtimeplatform-spark test -d "/data/lakehouse/silver/trades/_delta_log" 2>/dev/null; echo $?)
falling=$(docker logs realtimeplatform-spark-streaming --tail 20 2>/dev/null | grep -c "falling behind")

if [ "$bronze_ok" = "0" ] && [ "$silver_ok" = "0" ]; then
    if [ "$falling" -gt 0 ]; then
        echo "  ⚠️  Bronze + Silver activos pero batches lentos ($falling warnings)"
    else
        echo "  ✅ Bronze + Silver activos y estables"
    fi
else
    echo "  ⏳ Arrancando o sin datos aún"
fi

echo ""
echo "════════════════════════════════════════"
