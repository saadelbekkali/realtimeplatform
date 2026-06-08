"""
Genera el diagrama de arquitectura de la plataforma como PNG.
Uso: python3 scripts/generate_diagram.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

fig, ax = plt.subplots(figsize=(22, 13))
ax.set_xlim(0, 22)
ax.set_ylim(0, 13)
ax.axis("off")
BG = "#10131a"
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)

# ── Colores ───────────────────────────────────────────────────────────────────
C_EXT    = "#0d2137"   # azul muy oscuro  — externo (Binance)
C_DB     = "#0d2b1a"   # verde oscuro     — base de datos
C_MSG    = "#1a1a0d"   # amarillo oscuro  — mensajería
C_SPARK  = "#1a0d2b"   # violeta          — Spark
C_BRONZE = "#2b1500"   # marrón           — Bronze
C_SILVER = "#1a1a2b"   # azul gris        — Silver
C_GOLD   = "#2b2200"   # dorado           — Gold
C_ORCH   = "#2b0d2b"   # magenta oscuro   — Airflow
C_VIZ    = "#0d1f2b"   # azul viz         — Superset
C_VOL    = "#1e1e1e"   # gris             — volumen compartido

# Colores de borde por grupo
B_EXT    = "#3a7abf"
B_DB     = "#2ecc71"
B_MSG    = "#f1c40f"
B_SPARK  = "#9b59b6"
B_BRONZE = "#cd6f1f"
B_SILVER = "#7f8fa6"
B_GOLD   = "#f39c12"
B_ORCH   = "#e91e8c"
B_VIZ    = "#2980b9"
B_VOL    = "#555555"


def box(x, y, w, h, title, sub="", fc=C_DB, ec=B_DB, fs=9):
    r = FancyBboxPatch((x, y), w, h,
                       boxstyle="round,pad=0.1",
                       facecolor=fc, edgecolor=ec,
                       linewidth=1.6, alpha=0.92, zorder=3)
    ax.add_patch(r)
    cy = y + h / 2 + (0.17 if sub else 0)
    ax.text(x + w / 2, cy, title, ha="center", va="center",
            color="white", fontsize=fs, fontweight="bold", zorder=4)
    if sub:
        ax.text(x + w / 2, y + h / 2 - 0.20, sub, ha="center", va="center",
                color="#aaaaaa", fontsize=7, zorder=4)


def group_rect(x, y, w, h, label, ec="#444455", ls="--"):
    r = FancyBboxPatch((x, y), w, h,
                       boxstyle="round,pad=0.1",
                       facecolor="none", edgecolor=ec,
                       linewidth=1.0, linestyle=ls, zorder=1)
    ax.add_patch(r)
    ax.text(x + 0.18, y + h + 0.05, label, color=ec,
            fontsize=7.5, style="italic", zorder=5)


def arrow(x1, y1, x2, y2, label="", ec="#aaaaaa", lw=1.6):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=ec,
                                lw=lw, mutation_scale=13), zorder=2)
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        dx, dy = x2 - x1, y2 - y1
        # offset perpendicular
        import math
        length = math.hypot(dx, dy) or 1
        ox, oy = -dy / length * 0.25, dx / length * 0.25
        ax.text(mx + ox, my + oy, label, ha="center", va="center",
                color="#cccccc", fontsize=7, zorder=5)


def dashed_arrow(x1, y1, x2, y2, label="", ec="#e91e8c"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=ec, lw=1.4,
                                linestyle="dashed", mutation_scale=12), zorder=2)
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2 + 0.15
        ax.text(mx, my, label, ha="center", va="bottom",
                color=ec, fontsize=7, zorder=5)


# ══════════════════════════════════════════════════════════════════════════════
# EXTERNAL
# ══════════════════════════════════════════════════════════════════════════════
ax.text(1.2, 12.6, "EXTERNAL", color=B_EXT, fontsize=8, style="italic")
box(0.4, 11.2, 2.6, 1.1, "Binance", "REST API pública", C_EXT, B_EXT)

# ══════════════════════════════════════════════════════════════════════════════
# DOCKER COMPOSE — contenedor grande
# ══════════════════════════════════════════════════════════════════════════════
group_rect(3.4, 0.4, 18.1, 12.0, "Docker Compose  (realtimeplatform)", ec="#3a6abf", ls="-")

# ─── Profile: core ────────────────────────────────────────────────────────────
group_rect(3.7, 7.0, 14.4, 4.9, "profile: core", ec="#2ecc71")

# Generator
box(3.9, 10.0, 2.8, 1.2, "generator", "polls Binance → INSERT", C_DB, B_DB)

# PostgreSQL
box(7.4, 10.0, 3.0, 1.2, "postgres:15", "WAL logical replication", C_DB, B_DB)

# Debezium
box(11.1, 10.0, 3.0, 1.2, "debezium:2.4", "CDC connector", C_MSG, B_MSG)

# Zookeeper + Kafka (agrupados)
group_rect(14.7, 9.7, 3.0, 1.8, "Kafka stack", ec=B_MSG)
box(14.9, 10.0, 2.6, 1.2, "kafka + zookeeper", "topic: dbserver1.public.trades", C_MSG, B_MSG, fs=8)

# Spark container
box(3.9, 7.3, 3.4, 1.4, "spark container", "spark-submit main.py\nBronze + Silver stream", C_SPARK, B_SPARK, fs=8)

# connector-init (pequeño)
box(11.1, 7.3, 2.4, 1.0, "connector-init", "registra conector", C_DB, B_DB, fs=7.5)

# ─── Volumen compartido: lakehouse_data ──────────────────────────────────────
group_rect(3.7, 3.8, 10.6, 2.8, "volume: lakehouse_data  (Bronze · Silver · Gold)", ec=B_VOL)

box(3.9, 4.2, 2.8, 1.8, "Bronze",  "Delta Lake\nraw CDC events",   C_BRONZE, B_BRONZE, fs=8.5)
box(7.2, 4.2, 2.8, 1.8, "Silver",  "Delta Lake\nlimpio · tipado",  C_SILVER, B_SILVER, fs=8.5)
box(10.5, 4.2, 3.5, 0.8, "Gold · OHLCV",     "velas 1min",  C_GOLD, B_GOLD, fs=7.5)
box(10.5, 5.2, 3.5, 0.8, "Gold · Pressure",  "buy/sell 24h", C_GOLD, B_GOLD, fs=7.5)

# Gold ranking — fuera del grupo pero en el mismo volumen conceptual
box(10.5, 3.0, 3.5, 0.8, "Gold · Ranking",   "volumen 24h",  C_GOLD, B_GOLD, fs=7.5)

# ─── Profile: orchestration ───────────────────────────────────────────────────
group_rect(3.7, 0.7, 6.8, 2.8, "profile: orchestration", ec=B_ORCH)

box(3.9, 1.0, 2.8, 1.2, "airflow-scheduler", "DAG cada 30 min", C_ORCH, B_ORCH, fs=8)
box(7.1, 1.0, 3.0, 1.2, "airflow-webserver", ":8081", C_ORCH, B_ORCH, fs=8)
# airflow-postgres (pequeño)
box(3.9, 2.4, 2.8, 0.8, "airflow-postgres", "metadata DB", C_DB, B_DB, fs=7.5)

# ─── Profile: viz ─────────────────────────────────────────────────────────────
group_rect(11.0, 0.7, 4.6, 2.8, "profile: viz", ec=B_VIZ)
box(11.2, 1.0, 4.2, 1.2, "superset", ":8088  dashboards", C_VIZ, B_VIZ)

# ─── Profile: monitoring ──────────────────────────────────────────────────────
group_rect(16.2, 7.0, 5.0, 4.9, "profile: monitoring", ec="#888888")
box(16.4, 10.0, 2.4, 1.0, "kafka-ui",     ":8080",  C_MSG,  B_MSG,  fs=8)
box(16.4,  8.8, 2.4, 1.0, "debezium-ui",  ":8090",  C_MSG,  B_MSG,  fs=8)
box(16.4,  7.6, 2.4, 1.0, "pgadmin",      ":5050",  C_DB,   B_DB,   fs=8)

# ══════════════════════════════════════════════════════════════════════════════
# FLECHAS — flujo de datos
# ══════════════════════════════════════════════════════════════════════════════

# Binance → generator
arrow(3.0, 11.6, 3.9, 11.0, "polling 10s", B_EXT)

# generator → postgres
arrow(6.7, 10.6, 7.4, 10.6, "INSERT\nON CONFLICT", B_DB)

# postgres → debezium
arrow(10.4, 10.6, 11.1, 10.6, "WAL", B_MSG)

# debezium → kafka
arrow(14.1, 10.6, 14.7, 10.6, "CDC\nevents", B_MSG)

# connector-init → debezium (registro)
arrow(11.1, 7.8, 11.4, 10.0, "POST /connectors", B_DB)

# kafka → spark
arrow(15.5, 10.0, 6.6, 8.7, "readStream\n(Kafka)", B_SPARK)

# spark → Bronze
arrow(5.6, 7.3, 5.3, 6.0, "write\nDelta", B_BRONZE)

# Bronze → Silver (readStream Delta)
arrow(6.7, 5.1, 7.2, 5.1, "readStream\nDelta", B_SILVER)

# Silver → Gold (Airflow batch)
arrow(9.8, 5.1, 10.5, 5.6, "", B_GOLD)
arrow(9.8, 5.1, 10.5, 4.6, "", B_GOLD)
arrow(9.8, 4.5, 10.5, 3.4, "", B_GOLD)

# Airflow scheduler → spark (docker exec via docker.sock)
dashed_arrow(5.5, 2.2, 5.5, 7.3, "docker exec\nspark-submit run_gold.py", B_ORCH)

# Gold → Superset
arrow(13.5, 4.6, 13.5, 2.2, "", B_VIZ)
arrow(13.5, 2.2, 11.2, 2.2, "read Delta\n(lakehouse_data)", B_VIZ)

# monitoring — flechas punteadas a sus servicios
dashed_arrow(16.4, 10.5, 17.7, 10.5, "", "#888888")
dashed_arrow(14.7, 10.5, 16.4, 10.5, "", "#888888")
dashed_arrow(10.4, 10.5, 16.4, 9.3, "", "#888888")
dashed_arrow(7.4,  10.5, 16.4, 8.1, "", "#888888")

# ══════════════════════════════════════════════════════════════════════════════
# TÍTULO
# ══════════════════════════════════════════════════════════════════════════════
ax.text(11, 12.7, "Real-Time Data Platform",
        ha="center", va="center", color="white",
        fontsize=17, fontweight="bold")
ax.text(11, 12.3,
        "Binance  ·  PostgreSQL  ·  Debezium  ·  Kafka  ·  Spark Structured Streaming  ·  Delta Lake (Medallion)  ·  Airflow  ·  Superset",
        ha="center", va="center", color="#777788", fontsize=8)

plt.tight_layout(pad=0.3)
out = "architecture.png"
plt.savefig(out, dpi=160, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Guardado: {out}")
