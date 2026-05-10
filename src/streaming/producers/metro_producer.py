"""
metro_producer.py — Productor Kafka que reproduce la afluencia del Metro de
Medellín en tiempo real a partir del CSV histórico real (2022-2024).

Fuente: data/raw/metro_afluencia/afluencia_metro_*.csv
        formato largo: fecha, linea, hora, pasajeros
        (descargado y convertido por scripts/descargar_metro_afluencia_real.py)

Esto NO es sintético — es la afluencia real publicada por Metro de Medellín
en el portal de datos abiertos.

La propuesta original hablaba de validaciones por torniquete cada 30 s. Los
datos públicos están agregados por línea-hora, no por torniquete; este
productor distribuye uniformemente los pasajeros de una hora-línea entre
varios eventos sintéticos para producir un stream de granularidad fina
(decisión documentada en sprint-3-streaming-completo.md).

Tópico: `metro.validaciones`
Esquema JSON:
    {
      "linea": "LÍNEA A",
      "timestamp": "2024-03-15T07:42:13",
      "hora": 7,
      "pasajeros_periodo": 124,    // pasajeros en este micro-evento
      "pasajeros_hora_total": 15376,
      "fuente": "real_metro_afluencia"
    }

Variables:
    INTERVALO_S          (default 0.3)
    EVENTOS_POR_HORA     (default 12)   en cuántos micro-eventos partir cada hora-línea
    LIMITE_EVENTOS       (default 0)
"""

from __future__ import annotations

import csv
import glob
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaProducer
except ImportError:
    print("ERROR: pip install kafka-python", flush=True)
    sys.exit(1)

from shared.config import KAFKA_BOOTSTRAP, TOPIC_METRO

CSV_GLOB = "/workspace/data/raw/metro_afluencia/afluencia_metro_*.csv"
INTERVALO_S = float(os.getenv("INTERVALO_S", "0.3"))
EVENTOS_POR_HORA = max(1, int(os.getenv("EVENTOS_POR_HORA", "12")))
LIMITE_EVENTOS = int(os.getenv("LIMITE_EVENTOS", "0"))


def _generar_micro_eventos(fila: dict) -> list[dict]:
    """Reparte los pasajeros de una hora-línea en EVENTOS_POR_HORA micro-eventos."""
    try:
        fecha = datetime.fromisoformat(fila["fecha"])
        hora = int(fila["hora"])
        pasajeros = int(fila["pasajeros"])
    except (ValueError, KeyError):
        return []
    if pasajeros <= 0:
        return []
    linea = fila.get("linea", "DESCONOCIDA").strip()

    eventos = []
    base_ts = fecha.replace(hour=hora, minute=0, second=0)
    porcion = pasajeros / EVENTOS_POR_HORA
    for i in range(EVENTOS_POR_HORA):
        # Pequeña aleatoriedad para evitar valores idénticos
        variacion = random.uniform(0.7, 1.3)
        cantidad = max(0, int(round(porcion * variacion)))
        ts = base_ts + timedelta(minutes=int(60 * i / EVENTOS_POR_HORA))
        eventos.append({
            "linea": linea,
            "timestamp": ts.isoformat(timespec="seconds"),
            "hora": hora,
            "pasajeros_periodo": cantidad,
            "pasajeros_hora_total": pasajeros,
            "fuente": "real_metro_afluencia",
        })
    return eventos


def main() -> int:
    archivos = sorted(glob.glob(CSV_GLOB))
    if not archivos:
        print(f"ERROR: sin CSVs en {CSV_GLOB}", flush=True)
        return 1
    print(f"→ Kafka {KAFKA_BOOTSTRAP}  ·  tópico {TOPIC_METRO}", flush=True)
    print(f"→ Archivos: {len(archivos)}  ·  eventos_por_hora={EVENTOS_POR_HORA}  ·  intervalo={INTERVALO_S}s", flush=True)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=20,
    )

    enviados = 0
    for arch in archivos:
        print(f"  · {arch.split('/')[-1]}", flush=True)
        with open(arch, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for fila in reader:
                for evt in _generar_micro_eventos(fila):
                    producer.send(TOPIC_METRO, key=evt["linea"], value=evt)
                    enviados += 1
                    if enviados % 200 == 0:
                        print(f"    → enviados {enviados}", flush=True)
                    if LIMITE_EVENTOS and enviados >= LIMITE_EVENTOS:
                        producer.flush()
                        print(f"✅ Total enviados: {enviados}", flush=True)
                        return 0
                    time.sleep(INTERVALO_S)
    producer.flush()
    print(f"✅ Total enviados: {enviados}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n→ interrumpido", flush=True)
