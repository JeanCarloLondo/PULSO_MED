"""
metro_afluencia_job.py — Pregunta S-4.

Acumula validaciones del Metro por línea en ventana tumbling de 5 minutos y
emite snapshots a `afluencia_metro_rt`. Cuando la afluencia acumulada supera
el percentil 90 histórico para esa franja horaria — leído desde Gold via el
job híbrido — se genera una alerta.

Para mantener la responsabilidad enfocada, este job NO consulta Gold
directamente; sólo computa la agregación tumbling. La comparación con el
percentil histórico la hace `job_hibrido.py` (sección 4.3 propuesta), que
escribe en otra colección. Esto desacopla y permite testear cada uno
independientemente.

Variables:
    VENTANA_MINUTOS         (default 5)
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
    COL_METRO_RT,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
    TOPIC_METRO,
)

VENTANA_MIN = int(os.getenv("VENTANA_MINUTOS", "5"))

DETENER = False


def _handler(_sig, _frame):
    global DETENER
    DETENER = True


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


def _alinear(ts: datetime, mins: int) -> datetime:
    base = (ts.minute // mins) * mins
    return ts.replace(minute=base, second=0, microsecond=0)


def main() -> int:
    print(f"→ ventana={VENTANA_MIN}min", flush=True)
    print(f"→ Kafka {KAFKA_BOOTSTRAP}  ·  tópico {TOPIC_METRO}", flush=True)

    consumer = KafkaConsumer(
        TOPIC_METRO,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="pulsomed-metro",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,
    )
    mongo = MongoClient(MONGO_URI)
    coll = mongo[MONGO_DB][COL_METRO_RT]
    coll.create_index([("linea", 1), ("ventana_inicio", 1)], unique=True)

    buf: dict[tuple[str, datetime], int] = defaultdict(int)
    contador_ev: dict[tuple[str, datetime], int] = defaultdict(int)
    ultimo_event_ts = datetime.utcnow()

    print("→ Esperando eventos…", flush=True)

    while not DETENER:
        try:
            for msg in consumer:
                v = msg.value
                linea = (v.get("linea") or "").strip()
                if not linea:
                    continue
                pasajeros = int(v.get("pasajeros_periodo") or 0)
                try:
                    ts = datetime.fromisoformat(v.get("timestamp", "").replace("Z", ""))
                except Exception:
                    ts = datetime.utcnow()
                ultimo_event_ts = max(ultimo_event_ts, ts)
                vent = _alinear(ts, VENTANA_MIN)
                buf[(linea, vent)] += pasajeros
                contador_ev[(linea, vent)] += 1
                if DETENER:
                    break
        except Exception as exc:
            print(f"  ⚠ error consumiendo: {exc}", flush=True)

        cierre = max(ultimo_event_ts, datetime.utcnow()) - timedelta(minutes=VENTANA_MIN)
        a_cerrar = [k for k in buf if k[1] < cierre]
        for k in a_cerrar:
            linea, vent = k
            total = buf.pop(k)
            n_ev = contador_ev.pop(k)
            doc = {
                "linea": linea,
                "ventana_inicio": vent,
                "ventana_fin": vent + timedelta(minutes=VENTANA_MIN),
                "pasajeros_acumulados": total,
                "eventos_en_ventana": n_ev,
                "tipo": "snapshot_afluencia",
                "emitido_en": datetime.utcnow(),
            }
            try:
                coll.insert_one(doc)
                if total > 0 and n_ev >= 5:
                    print(
                        f"  🚇 {linea:<12} ventana={vent:%H:%M}  pax={total:6,}  evts={n_ev}",
                        flush=True,
                    )
            except Exception as exc:
                m = str(exc).lower()
                if "duplicate" not in m and "e11000" not in m:
                    print(f"  ⚠ Mongo: {exc}", flush=True)

    consumer.close()
    mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
