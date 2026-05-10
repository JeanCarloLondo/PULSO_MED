"""
encicla_producer.py — Productor Kafka que simula la disponibilidad de
estaciones EnCicla en tiempo real.

Por qué simulado y no API real:
    El Área Metropolitana no expone una API pública de disponibilidad. La app
    móvil de EnCicla usa un backend privado autenticado. Este productor parte
    de las 80 estaciones reales (con nombre, coordenadas y capacidad reales,
    obtenidas de OpenStreetMap vía scripts/descargar_encicla_estaciones.py) y
    simula la dinámica de demanda con un modelo de Poisson + perfil horario.

Lógica de la simulación (honesta y documentada en docs/sprints/sprint-3-*.md):
    - Cada estación tiene `capacidad_anclajes` (real, OSM).
    - Ratio de ocupación inicial: 50 % (mitad bicis, mitad anclajes vacíos).
    - Cada tick simula entradas/salidas con λ que depende de:
        · franja horaria (pico AM/PM aumenta movimiento)
        · día de semana (finde reduce ~30 %)
    - Se inyecta un evento "drenaje" cada `INYECTAR_PICO_CADA` ticks en una
      estación aleatoria: bicis → ≤1 (para garantizar que el job de alerta S-1
      se dispare durante un demo corto).

Tópico: `encicla.disponibilidad`
Esquema JSON:
    {
      "estacion_id": "ENC042",
      "nombre": "Estación EnCicla Ruta N",
      "latitud": 6.2651, "longitud": -75.5663,
      "capacidad_anclajes": 26,
      "bicicletas_disponibles": 14,
      "anclajes_libres": 12,
      "timestamp": "2026-05-09T18:30:00"
    }

Uso:
    docker compose exec stream-runner \
      python /workspace/src/streaming/producers/encicla_producer.py
"""

from __future__ import annotations

import json
import os
import random
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

from shared.config import KAFKA_BOOTSTRAP, TOPIC_ENCICLA

ESTACIONES_JSON = Path("/workspace/data/raw/encicla_estaciones/estaciones_encicla.json")
INTERVALO_S = float(os.getenv("INTERVALO_S", "1.0"))
INYECTAR_PICO_CADA = int(os.getenv("INYECTAR_PICO_CADA", "20"))
LIMITE_EVENTOS = int(os.getenv("LIMITE_EVENTOS", "0"))


def _cargar_estaciones() -> list[dict]:
    if not ESTACIONES_JSON.exists():
        print(f"ERROR: falta {ESTACIONES_JSON}. Correr scripts/descargar_encicla_estaciones.py", flush=True)
        sys.exit(2)
    data = json.loads(ESTACIONES_JSON.read_text(encoding="utf-8"))
    return data["result"]["records"]


def _factor_horario(hora: int, dia_semana: int) -> float:
    """Multiplicador de demanda según franja horaria (pico AM 6-9, PM 17-20)."""
    base = 1.0
    if hora in (6, 7, 8, 17, 18, 19):
        base = 1.8
    elif hora in (9, 10, 16, 20):
        base = 1.2
    elif hora < 5 or hora > 22:
        base = 0.2
    if dia_semana >= 5:  # sábado/domingo
        base *= 0.7
    return base


def _simular_tick(estado: dict, ahora: datetime) -> tuple[int, int]:
    """Avanza el estado de una estación 1 unidad de tiempo (1 minuto simulado)."""
    cap = estado["capacidad_anclajes"]
    bicis = estado["_bicis"]
    factor = _factor_horario(ahora.hour, ahora.weekday())
    salidas = min(bicis, _muestrear_poisson(0.6 * factor))
    llegadas = min(cap - (bicis - salidas), _muestrear_poisson(0.6 * factor))
    bicis = max(0, min(cap, bicis - salidas + llegadas))
    estado["_bicis"] = bicis
    return bicis, cap - bicis


def _muestrear_poisson(lam: float) -> int:
    """Aproximación rápida sin numpy: composición exponencial."""
    L = pow(2.71828, -lam)
    k = 0
    p = 1.0
    while True:
        k += 1
        p *= random.random()
        if p < L:
            return k - 1


def main() -> int:
    estaciones = _cargar_estaciones()
    print(f"→ {len(estaciones)} estaciones reales (fuente: OSM)", flush=True)
    print(f"→ Kafka bootstrap: {KAFKA_BOOTSTRAP}  ·  tópico: {TOPIC_ENCICLA}", flush=True)

    # Inicializar estado en memoria (50 % ocupación)
    estado = {
        e["estacion_id"]: {
            **e,
            "_bicis": int(e["capacidad_anclajes"] * 0.5),
        }
        for e in estaciones
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        linger_ms=20,
    )

    enviados = 0
    while True:
        ahora = datetime.utcnow()
        for est_id, est in estado.items():
            bicis, libres = _simular_tick(est, ahora)

            # Pico de drenaje: cada N eventos vaciamos una estación al azar
            if INYECTAR_PICO_CADA > 0 and (enviados + 1) % INYECTAR_PICO_CADA == 0:
                drenada = random.choice(list(estado.keys()))
                estado[drenada]["_bicis"] = 1
                if drenada == est_id:
                    bicis = 1
                    libres = est["capacidad_anclajes"] - 1
                    print(f"  ⚡ Drenaje inyectado en {drenada}", flush=True)

            msg = {
                "estacion_id": est_id,
                "nombre": est["nombre"],
                "latitud": est["latitud"],
                "longitud": est["longitud"],
                "capacidad_anclajes": est["capacidad_anclajes"],
                "bicicletas_disponibles": bicis,
                "anclajes_libres": libres,
                "timestamp": ahora.isoformat(timespec="seconds"),
            }
            producer.send(TOPIC_ENCICLA, key=est_id, value=msg)
            enviados += 1

            if LIMITE_EVENTOS and enviados >= LIMITE_EVENTOS:
                producer.flush()
                print(f"✅ Total enviados: {enviados}", flush=True)
                return 0

        if enviados % 200 == 0:
            print(f"  → enviados {enviados} eventos (estaciones cubiertas: {len(estado)})", flush=True)

        producer.flush()
        time.sleep(INTERVALO_S)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n→ interrumpido por usuario", flush=True)
