"""
siata_producer.py — Productor Kafka que simula el stream SIATA en tiempo real.

Lee del histórico SIATA (data/raw/siata_historico/siata_pm25_*.csv), itera
cronológicamente y publica cada lectura en el tópico `siata.lecturas` de
Kafka. En modo dev acelera el tiempo: una lectura cada `INTERVALO_S`
segundos (default 1s) en vez de cada hora real.

Para inducir alertas durante una demo corta, opcionalmente inyecta
"eventos pico" donde PM2.5 sube por encima del umbral configurable.

Uso (desde el contenedor stream-runner):
    docker compose exec stream-runner python /workspace/src/streaming/producers/siata_producer.py

Variables de entorno:
    KAFKA_BOOTSTRAP_SERVERS   (default: kafka:9092)
    INTERVALO_S               (default: 1.0)  segundos entre eventos
    INYECTAR_PICO_CADA        (default: 30)   cada N eventos meter un pico
    PICO_PM25                 (default: 95.0) valor del pico forzado
    LIMITE_EVENTOS            (default: 0)    0 = infinito; N corta tras N eventos
"""

from __future__ import annotations

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
    print("ERROR: falta kafka-python. Instalar: pip install kafka-python", flush=True)
    sys.exit(1)

from shared.config import KAFKA_BOOTSTRAP, TOPIC_SIATA

CSV_DIR = "/workspace/data/raw/siata_historico"
INTERVALO_S = float(os.getenv("INTERVALO_S", "1.0"))
INYECTAR_PICO_CADA = int(os.getenv("INYECTAR_PICO_CADA", "30"))
PICO_PM25 = float(os.getenv("PICO_PM25", "95.0"))
LIMITE_EVENTOS = int(os.getenv("LIMITE_EVENTOS", "0"))


def cargar_lecturas() -> list[dict]:
    archivos = sorted(Path(CSV_DIR).glob("siata_pm25_*.csv"))
    if not archivos:
        print(f"ERROR: sin CSV en {CSV_DIR}", flush=True)
        sys.exit(2)
    lecturas: list[dict] = []
    for arch in archivos:
        with open(arch, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lecturas.append(row)
    print(f"Cargadas {len(lecturas):,} lecturas de {len(archivos)} archivos", flush=True)
    return lecturas


def normalizar(row: dict) -> dict:
    """Convierte la fila CSV a la forma canónica del tópico."""
    def f(k):
        v = row.get(k, "")
        try:
            return float(v) if v not in ("", None) else None
        except (TypeError, ValueError):
            return None

    return {
        "estacion_id": row.get("estacion_id", ""),
        "estacion_nombre": row.get("estacion_nombre", ""),
        "zona": row.get("zona", ""),
        "latitud": f("latitud"),
        "longitud": f("longitud"),
        "timestamp": row.get("timestamp", ""),
        "pm25": f("pm25"),
        "pm10": f("pm10"),
        "temperatura_c": f("temperatura_c"),
        "humedad_pct": f("humedad_pct"),
        "precipitacion_mm": f("precipitacion_mm"),
        "viento_kmh": f("viento_kmh"),
    }


def main() -> int:
    print(f"→ Bootstrap Kafka: {KAFKA_BOOTSTRAP}", flush=True)
    print(f"→ Tópico: {TOPIC_SIATA}", flush=True)
    print(f"→ Intervalo: {INTERVALO_S}s entre eventos", flush=True)
    print(f"→ Pico PM2.5={PICO_PM25} cada {INYECTAR_PICO_CADA} eventos", flush=True)
    print(f"→ Límite eventos: {LIMITE_EVENTOS or 'infinito'}", flush=True)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=20,
    )

    lecturas = cargar_lecturas()

    enviados = 0
    for raw in lecturas:
        msg = normalizar(raw)

        # Inyección de pico para asegurar al menos una alerta durante la demo
        if INYECTAR_PICO_CADA > 0 and (enviados + 1) % INYECTAR_PICO_CADA == 0:
            msg["pm25"] = PICO_PM25
            msg["timestamp"] = datetime.utcnow().isoformat(timespec="seconds")
            print(f"  ⚡ Pico inyectado: {msg['estacion_id']} pm25={PICO_PM25}", flush=True)

        producer.send(TOPIC_SIATA, key=msg["estacion_id"], value=msg)
        enviados += 1

        if enviados % 50 == 0:
            print(f"  → enviados {enviados} eventos", flush=True)

        if LIMITE_EVENTOS and enviados >= LIMITE_EVENTOS:
            break

        time.sleep(INTERVALO_S)

    producer.flush()
    print(f"✅ Total enviados: {enviados}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
