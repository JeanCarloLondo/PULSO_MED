"""
encicla_disponibilidad_job.py — Pregunta S-1.

Detecta estaciones EnCicla con disponibilidad crítica (≤ UMBRAL bicis) en una
ventana sliding y persiste el snapshot a MongoDB para que el dashboard pueda
mostrar el estado actual de la red.

Patrón (heredado del Sprint 2):
    - Consume `encicla.disponibilidad`.
    - Mantiene buffer por (estacion_id, ventana_inicio) con la lista de bicis
      disponibles vista en la ventana.
    - Ventana SLIDING: tamaño VENTANA_MINUTOS, paso PASO_SEGUNDOS.
    - Cuando una ventana cierra, calcula min/avg/last bicis y emite documento a
      Mongo. Si min ≤ UMBRAL_BICIS → tipo=ALERTA_DISPONIBILIDAD.
    - Índice único {estacion_id, ventana_inicio} evita duplicados al reprocesar.

Decisión técnica:
    Sliding 1 min / 30 s coincide con la propuesta original (sección 4.2). El
    sliding step (30 s) es la frecuencia con la que el dashboard refresca; el
    tamaño (1 min) es el suavizado contra picos espurios.

Variables:
    VENTANA_MINUTOS    (default 1)
    PASO_SEGUNDOS      (default 30)
    UMBRAL_BICIS       (default 2)

Uso:
    docker compose exec stream-runner \
      python /workspace/src/streaming/flink_jobs/encicla_disponibilidad_job.py
"""

from __future__ import annotations

import json
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaConsumer
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pip install kafka-python pymongo", flush=True)
    sys.exit(1)

from shared.config import (
    COL_ENCICLA_DISP,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
    TOPIC_ENCICLA,
)

VENTANA_MIN = int(os.getenv("VENTANA_MINUTOS", "1"))
PASO_SEG = int(os.getenv("PASO_SEGUNDOS", "30"))
UMBRAL_BICIS = int(os.getenv("UMBRAL_BICIS", "2"))

DETENER = False


def _handler(_sig, _frame):
    global DETENER
    DETENER = True


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


def _alinear_ventana(ts: datetime, paso_seg: int) -> datetime:
    """Alinea al boundary inferior del paso sliding."""
    epoch = (ts - datetime(1970, 1, 1)).total_seconds()
    base = (int(epoch) // paso_seg) * paso_seg
    return datetime.utcfromtimestamp(base)


def main() -> int:
    print(
        f"→ ventana={VENTANA_MIN}m paso={PASO_SEG}s umbral={UMBRAL_BICIS}",
        flush=True,
    )
    print(f"→ Kafka {KAFKA_BOOTSTRAP}  ·  tópico {TOPIC_ENCICLA}", flush=True)
    print(f"→ Mongo {MONGO_DB}/{COL_ENCICLA_DISP}", flush=True)

    consumer = KafkaConsumer(
        TOPIC_ENCICLA,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="pulsomed-encicla",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,
    )
    mongo = MongoClient(MONGO_URI)
    coll = mongo[MONGO_DB][COL_ENCICLA_DISP]
    coll.create_index([("estacion_id", 1), ("ventana_inicio", 1)], unique=True)

    # buffer: (est_id, ventana_inicio) → list[(ts, bicis)]
    buf: dict[tuple[str, datetime], list[tuple[datetime, int]]] = defaultdict(list)
    metadata: dict[str, dict] = {}
    ultimo_event_ts = datetime.utcnow()

    print("→ Esperando mensajes... (Ctrl+C para detener)", flush=True)

    while not DETENER:
        try:
            for msg in consumer:
                v = msg.value
                est_id = v.get("estacion_id")
                if not est_id:
                    continue
                bicis = int(v.get("bicicletas_disponibles", 0))
                try:
                    ts = datetime.fromisoformat(v["timestamp"])
                except Exception:
                    ts = datetime.utcnow()
                ultimo_event_ts = max(ultimo_event_ts, ts)
                # cada evento entra en TODAS las ventanas sliding que lo contienen
                ventana_inicio = _alinear_ventana(ts, PASO_SEG)
                # generamos las K ventanas que aún incluyen este timestamp
                for k in range(VENTANA_MIN * 60 // PASO_SEG):
                    inicio = ventana_inicio - timedelta(seconds=PASO_SEG * k)
                    fin = inicio + timedelta(minutes=VENTANA_MIN)
                    if inicio <= ts < fin:
                        buf[(est_id, inicio)].append((ts, bicis))
                metadata[est_id] = {
                    "nombre": v.get("nombre", ""),
                    "latitud": v.get("latitud"),
                    "longitud": v.get("longitud"),
                    "capacidad_anclajes": v.get("capacidad_anclajes"),
                }
                if DETENER:
                    break
        except Exception as exc:
            print(f"  ⚠ error consumiendo: {exc}", flush=True)

        # Cierre por watermark suave (event-time o wall-clock)
        watermark = max(ultimo_event_ts, datetime.utcnow()) - timedelta(minutes=VENTANA_MIN)
        a_cerrar = [k for k in buf if k[1] < watermark]
        for k in a_cerrar:
            est_id, inicio = k
            valores = buf.pop(k)
            if not valores:
                continue
            bicis_serie = [b for _, b in valores]
            bicis_min = min(bicis_serie)
            bicis_avg = sum(bicis_serie) / len(bicis_serie)
            ult_ts, ult_b = valores[-1]
            meta = metadata.get(est_id, {})
            doc = {
                "estacion_id": est_id,
                "nombre": meta.get("nombre", ""),
                "latitud": meta.get("latitud"),
                "longitud": meta.get("longitud"),
                "capacidad_anclajes": meta.get("capacidad_anclajes"),
                "ventana_inicio": inicio,
                "ventana_fin": inicio + timedelta(minutes=VENTANA_MIN),
                "bicicletas_min": bicis_min,
                "bicicletas_avg": round(bicis_avg, 2),
                "bicicletas_ultimo": ult_b,
                "lecturas": len(valores),
                "tipo": "ALERTA_DISPONIBILIDAD" if bicis_min <= UMBRAL_BICIS else "snapshot_disponibilidad",
                "umbral": UMBRAL_BICIS,
                "emitido_en": datetime.utcnow(),
            }
            try:
                coll.insert_one(doc)
                if doc["tipo"] == "ALERTA_DISPONIBILIDAD":
                    print(
                        f"  🚲 ALERTA  {est_id} ({meta.get('nombre','')[:30]:<30})"
                        f"  bicis_min={bicis_min}  ventana={inicio:%H:%M:%S}",
                        flush=True,
                    )
            except Exception as exc:
                m = str(exc).lower()
                if "duplicate" not in m and "e11000" not in m:
                    print(f"  ⚠ Mongo: {exc}", flush=True)

    print(f"→ cierre. ventanas pendientes: {len(buf)}", flush=True)
    consumer.close()
    mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
