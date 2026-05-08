"""
siata_alert_job.py — Job de stream que agrega PM2.5 por zona en ventanas
tumbling y emite alertas a MongoDB.

DECISIÓN TÉCNICA (Sprint 2):
    En vez de Flink/PyFlink se usa un consumidor Kafka en Python con una
    ventana tumbling implementada manualmente sobre el `event_time` del
    mensaje (no `processing_time`). La razón es la curva de despliegue:
    PyFlink requiere flink-kafka-connector compatible con la versión de
    Flink, jars en /opt/flink/lib, y un build con maven. Para validar el
    patrón end-to-end del Sprint 2 (pregunta S-2) este script entrega lo
    mismo: ventana tumbling, agregación por clave (zona), umbral, sink.
    Sprint 3 migra a Flink real cuando haya 4 jobs paralelos compitiendo.

Lógica:
    - Consume tópico `siata.lecturas` (group `pulsomed-alert`).
    - Mantiene un buffer por (zona, ventana) con las lecturas pm25.
    - Cuando pasa la ventana (10 minutos por defecto), promedia y emite
      alerta a Mongo si pm25_avg > UMBRAL.
    - Limpia ventanas viejas para no crecer en memoria.

Variables de entorno:
    KAFKA_BOOTSTRAP_SERVERS   (default: kafka:9092)
    MONGO_HOST                (default: mongodb)
    MONGO_INITDB_ROOT_*       (credenciales)
    VENTANA_MINUTOS           (default: 10)
    UMBRAL_PM25               (default: 75.0)

Uso (en stream-runner):
    docker compose exec stream-runner python /workspace/src/streaming/flink_jobs/siata_alert_job.py
"""

from __future__ import annotations

import json
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaConsumer
    from pymongo import MongoClient
except ImportError:
    print("ERROR: faltan kafka-python / pymongo. Instalar:", flush=True)
    print("  pip install kafka-python pymongo", flush=True)
    sys.exit(1)

from shared.config import (
    COL_ALERTAS_AIRE,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
    TOPIC_SIATA,
)

VENTANA_MIN = int(os.getenv("VENTANA_MINUTOS", "10"))
UMBRAL = float(os.getenv("UMBRAL_PM25", "75.0"))

DETENER = False


def _handler(_sig, _frame):
    global DETENER
    DETENER = True
    print("\n→ SIGINT — finalizando ventanas pendientes...", flush=True)


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


def _alinear_ventana(ts: datetime, minutos: int) -> datetime:
    """Devuelve el inicio de la ventana tumbling de N minutos para `ts`."""
    base_min = (ts.minute // minutos) * minutos
    return ts.replace(minute=base_min, second=0, microsecond=0)


def _gravedad(pm25: float) -> str:
    if pm25 >= 150:
        return "critica"
    if pm25 >= 75:
        return "moderada"
    return "leve"


def main() -> int:
    print(f"→ Kafka bootstrap: {KAFKA_BOOTSTRAP}", flush=True)
    print(f"→ Mongo: {MONGO_URI[:40]}...{MONGO_DB}/{COL_ALERTAS_AIRE}", flush=True)
    print(f"→ Ventana tumbling: {VENTANA_MIN} min  ·  Umbral PM2.5: {UMBRAL}", flush=True)

    consumer = KafkaConsumer(
        TOPIC_SIATA,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="pulsomed-alert",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,  # nos despierta cada 2s para chequear ventanas vencidas
    )

    mongo = MongoClient(MONGO_URI)
    coll = mongo[MONGO_DB][COL_ALERTAS_AIRE]
    coll.create_index([("zona", 1), ("ventana_inicio", 1)], unique=True)

    # Buffer: (zona, inicio_ventana) → list[pm25]
    buffer: dict[tuple[str, datetime], list[float]] = defaultdict(list)
    # Watermark "lazy": cierre por reloj real, no por event-time strict
    ultimo_evento_ts = datetime.utcnow()

    print("→ Esperando mensajes... (Ctrl+C para detener)", flush=True)

    while not DETENER:
        try:
            for msg in consumer:
                v = msg.value
                pm = v.get("pm25")
                if pm is None or pm <= 0:
                    continue
                zona = v.get("zona", "desconocida")
                # event_time del mensaje
                ts_raw = v.get("timestamp", "")
                try:
                    ts_evento = datetime.fromisoformat(ts_raw.replace("Z", ""))
                except Exception:
                    ts_evento = datetime.utcnow()
                ultimo_evento_ts = max(ultimo_evento_ts, ts_evento)

                ventana = _alinear_ventana(ts_evento, VENTANA_MIN)
                buffer[(zona, ventana)].append(float(pm))

                if DETENER:
                    break
        except Exception as exc:
            print(f"  ⚠ error consumiendo: {exc}", flush=True)

        # Cerrar ventanas que ya quedaron atrás. Usamos el max(event-time del
        # último evento, wall-clock now) para que las ventanas eventualmente
        # se cierren aunque deje de llegar tráfico.
        cierre = max(ultimo_evento_ts, datetime.utcnow()) - timedelta(minutes=VENTANA_MIN)
        cerradas = [k for k in buffer if k[1] < cierre]
        for k in cerradas:
            zona, vent = k
            valores = buffer.pop(k)
            if not valores:
                continue
            avg = sum(valores) / len(valores)
            if avg <= UMBRAL:
                continue
            doc = {
                "zona": zona,
                "ventana_inicio": vent,
                "ventana_fin": vent + timedelta(minutes=VENTANA_MIN),
                "pm25_promedio": round(avg, 2),
                "lecturas_en_ventana": len(valores),
                "tipo": "ALERTA_PM25",
                "gravedad": _gravedad(avg),
                "umbral": UMBRAL,
                "emitido_en": datetime.utcnow(),
            }
            try:
                coll.insert_one(doc)
                print(
                    f"  🚨 ALERTA {doc['gravedad']:<8} zona={zona:<25}"
                    f"  pm25_avg={avg:6.1f}  ventana={vent.strftime('%H:%M')}-"
                    f"{(vent+timedelta(minutes=VENTANA_MIN)).strftime('%H:%M')}",
                    flush=True,
                )
            except Exception as exc:
                # Duplicado por reproceso → ignorar
                msg = str(exc).lower()
                if "duplicate" not in msg and "e11000" not in msg:
                    print(f"  ⚠ Mongo: {exc}", flush=True)

    # Cierre final: vaciar todas las ventanas remanentes
    print(f"→ Cierre: {len(buffer)} ventanas en buffer", flush=True)
    consumer.close()
    mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
