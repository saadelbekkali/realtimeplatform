#!/bin/bash
set -e

echo "[superset] Migrando base de datos..."
superset db upgrade

echo "[superset] Creando usuario admin..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@admin.com \
  --password admin123 || echo "[superset] Usuario ya existe, continuando..."

echo "[superset] Inicializando roles y permisos..."
superset init

echo "[superset] Arrancando servidor..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads
