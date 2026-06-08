#!/usr/bin/env bash
# Registra el conector Debezium una vez el stack esté levantado

DEBEZIUM_URL="http://debezium:8083/connectors"
CONNECTOR_FILE="/connector.json"

echo "Esperando a que Debezium esté listo..."
until curl -s -o /dev/null -w "%{http_code}" "$DEBEZIUM_URL" | grep -q "200"; do
    sleep 3
done

echo "Registrando conector..."
curl -s -X POST "$DEBEZIUM_URL" \
     -H "Content-Type: application/json" \
     -d @"$CONNECTOR_FILE"

echo ""
echo "Estado del conector:"
curl -s "$DEBEZIUM_URL/postgres-connector/status"
