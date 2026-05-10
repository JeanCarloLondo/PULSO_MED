"""
simm_producer.py — Productor Kafka que reproduce los conteos vehiculares
del SIMM (cámaras INDRA) a partir del CSV histórico real.

Fuente: data/raw/simm_aforos/simm_traffic_data.csv (descargado de medata.gov.co)
Esto NO es sintético — son lecturas reales de cámaras CCTV. Cada fila trae
INTENSIDAD (vehículos por periodo), VELOCIDAD, CORREDOR, LOCATION (lat/lon).

Tópico: `simm.aforos`
Esquema JSON:
    {
      "corredor": "Carrera 70",
      "dispositivo": "CCTV",
      "latitud": 6.222705,
      "longitud": -75.591957,
      "timestamp": "2021-01-01T00:00:00",
      "intensidad": 11,        // vehículos en el periodo
      "velocidad_kmh": 24.0,
      "ocupacion_pct": 13
    }

Variables:
    INTERVALO_S            (default 1.0)   segundos entre eventos enviados
    LIMITE_EVENTOS         (default 0)     0 = recorre todo el CSV
    INYECTAR_PICO_CADA     (default 30)    cada N eventos amplifica intensidad

Uso:
    docker compose exec stream-runner \
      python /workspace/src/streaming/producers/simm_producer.py
"""

from __future__ import annotations

import ast
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaProducer
except ImportError:
    print("ERROR: pip install kafka-python", flush=True)
    sys.exit(1)

from shared.config import KAFKA_BOOTSTRAP, TOPIC_SIMM

CSV_PATH = Path("/workspace/data/raw/simm_aforos/simm_traffic_data.csv")
INTERVALO_S = float(os.getenv("INTERVALO_S", "1.0"))
LIMITE_EVENTOS = int(os.getenv("LIMITE_EVENTOS", "0"))
INYECTAR_PICO_CADA = int(os.getenv("INYECTAR_PICO_CADA", "30"))
PICO_INTENSIDAD = int(os.getenv("PICO_INTENSIDAD", "180"))


def _parsear_location(loc: str) -> tuple[float | None, float | None]:
    """LOCATION viene como dict-string Python: \"{'lon': '-75.5', 'lat': '6.2'}\""""
    if not loc:
        return None, None
    try:
        d = ast.literal_eval(loc)
        return float(d.get("lon")), float(d.get("lat"))
    except Exception:
        return None, None


def _normalizar(row: dict) -> dict:
    lon, lat = _parsear_location(row.get("LOCATION", ""))
    try:
        intensidad = int(row.get("INTENSIDAD") or 0)
    except ValueError:
        intensidad = 0
    try:
        velocidad = float(row.get("VELOCIDAD") or 0.0)
    except ValueError:
        velocidad = 0.0
    try:
        ocupacion = int(row.get("OCUPACION") or 0)
    except ValueError:
        ocupacion = 0
    return {
        "corredor": (row.get("CORREDOR") or "DESCONOCIDO").strip(),
        "dispositivo": row.get("DISPOSITIVO", "CCTV"),
        "latitud": lat,
        "longitud": lon,
        "timestamp": row.get("FECHAHORA", ""),
        "intensidad": intensidad,
        "velocidad_kmh": velocidad,
        "ocupacion_pct": ocupacion,
    }


def main() -> int:
    if not CSV_PATH.exists():
        print(f"ERROR: falta {CSV_PATH}", flush=True)
        return 1

    print(f"→ Kafka {KAFKA_BOOTSTRAP}  ·  tópico {TOPIC_SIMM}", flush=True)
    print(f"→ Fuente: {CSV_PATH.name}  ·  intervalo {INTERVALO_S}s", flush=True)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=20,
    )

    enviados = 0
    with CSV_PATH.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = _normalizar(row)
            if msg["intensidad"] == 0 and msg["velocidad_kmh"] == 0:
                continue
            if INYECTAR_PICO_CADA > 0 and (enviados + 1) % INYECTAR_PICO_CADA == 0:
                msg["intensidad"] = PICO_INTENSIDAD
                msg["timestamp"] = datetime.utcnow().isoformat(timespec="seconds")
                print(f"  ⚡ Pico inyectado: {msg['corredor']} intensidad={PICO_INTENSIDAD}", flush=True)
            producer.send(TOPIC_SIMM, key=msg["corredor"], value=msg)
            enviados += 1
            if enviados % 100 == 0:
                print(f"  → enviados {enviados} eventos", flush=True)
            if LIMITE_EVENTOS and enviados >= LIMITE_EVENTOS:
                break
            time.sleep(INTERVALO_S)

    producer.flush()
    print(f"✅ Total enviados: {enviados}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n→ interrumpido", flush=True)
