"""
simm_aforo_job.py — Pregunta S-3.

Agrega aforos vehiculares por corredor en ventanas tumbling de 5 min y emite
una alerta operacional cuando un corredor de **alta siniestralidad histórica**
muestra volumen sostenido elevado (>= UMBRAL_INTENSIDAD vehículos/min).

La lista de corredores de alta siniestralidad se carga al arranque desde un
JSON exportado por scripts/exportar_corredores_riesgo.py (que a su vez consulta
gold.corredores_riesgo_compuesto). Si el archivo no existe se usa una lista
hardcoded conservadora con corredores conocidos del Valle (Av. Las Vegas,
Carrera 80, Calle Colombia, Carrera 70, Calle Colombia, Av. Oriental).

Patrón:
    - Tópico `simm.aforos`, group `pulsomed-simm`.
    - Buffer (corredor, ventana_inicio) → list[intensidad].
    - Cierre por max(event_time, wall_clock) - VENTANA.
    - Sink: `aforos_corredor` (snapshot) + alerta cuando aplica.

Variables:
    VENTANA_MINUTOS         (default 5)
    UMBRAL_INTENSIDAD       (default 80)   vehículos/min agregados
"""

from __future__ import annotations

import json
import os
import signal
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaConsumer
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pip install kafka-python pymongo", flush=True)
    sys.exit(1)

from shared.config import (
    COL_AFOROS_CORREDOR,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
    TOPIC_SIMM,
)

VENTANA_MIN = int(os.getenv("VENTANA_MINUTOS", "5"))
UMBRAL = int(os.getenv("UMBRAL_INTENSIDAD", "80"))
JSON_RIESGO = Path("/workspace/data/processed/corredores_alta_siniestralidad.json")
CORREDORES_FALLBACK = {
    "Carrera 70", "Calle Colombia", "Avenida Las Vegas",
    "Carrera 80", "Avenida Oriental", "Carrera 65",
    "Calle 33", "Avenida 33",
}

DETENER = False


def _handler(_sig, _frame):
    global DETENER
    DETENER = True


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


def _alinear(ts: datetime, mins: int) -> datetime:
    base = (ts.minute // mins) * mins
    return ts.replace(minute=base, second=0, microsecond=0)


def _cargar_corredores_riesgo() -> set[str]:
    if JSON_RIESGO.exists():
        try:
            data = json.loads(JSON_RIESGO.read_text(encoding="utf-8"))
            corredores = {c.strip() for c in data.get("corredores", []) if c}
            if corredores:
                return corredores
        except Exception as exc:
            print(f"  ⚠ No pude leer {JSON_RIESGO}: {exc}", flush=True)
    print(f"  ⚠ Usando lista fallback ({len(CORREDORES_FALLBACK)} corredores)", flush=True)
    return CORREDORES_FALLBACK


def main() -> int:
    print(f"→ ventana={VENTANA_MIN}min  umbral_intensidad={UMBRAL}", flush=True)
    riesgo = _cargar_corredores_riesgo()
    print(f"→ Corredores alta siniestralidad cargados: {len(riesgo)}", flush=True)

    consumer = KafkaConsumer(
        TOPIC_SIMM,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="pulsomed-simm",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,
    )
    mongo = MongoClient(MONGO_URI)
    coll = mongo[MONGO_DB][COL_AFOROS_CORREDOR]
    coll.create_index([("corredor", 1), ("ventana_inicio", 1)], unique=True)

    buf: dict[tuple[str, datetime], list[int]] = defaultdict(list)
    coords: dict[str, tuple[float | None, float | None]] = {}
    ultimo_event_ts = datetime.utcnow()

    print("→ Esperando eventos…", flush=True)
    while not DETENER:
        try:
            for msg in consumer:
                v = msg.value
                corredor = (v.get("corredor") or "").strip()
                if not corredor:
                    continue
                intensidad = int(v.get("intensidad") or 0)
                try:
                    ts = datetime.fromisoformat((v.get("timestamp") or "").replace("Z", ""))
                except Exception:
                    ts = datetime.utcnow()
                ultimo_event_ts = max(ultimo_event_ts, ts)
                ventana = _alinear(ts, VENTANA_MIN)
                buf[(corredor, ventana)].append(intensidad)
                if v.get("latitud") is not None and v.get("longitud") is not None:
                    coords[corredor] = (v["latitud"], v["longitud"])
                if DETENER:
                    break
        except Exception as exc:
            print(f"  ⚠ error consumiendo: {exc}", flush=True)

        cierre = max(ultimo_event_ts, datetime.utcnow()) - timedelta(minutes=VENTANA_MIN)
        a_cerrar = [k for k in buf if k[1] < cierre]
        for k in a_cerrar:
            corredor, vent = k
            valores = buf.pop(k)
            if not valores:
                continue
            total = sum(valores)
            promedio = total / max(1, len(valores))
            por_minuto = total / VENTANA_MIN
            es_alerta = (corredor in riesgo) and (por_minuto >= UMBRAL)
            lat, lon = coords.get(corredor, (None, None))
            doc = {
                "corredor": corredor,
                "latitud": lat,
                "longitud": lon,
                "ventana_inicio": vent,
                "ventana_fin": vent + timedelta(minutes=VENTANA_MIN),
                "intensidad_total": total,
                "intensidad_promedio": round(promedio, 2),
                "intensidad_por_minuto": round(por_minuto, 2),
                "lecturas": len(valores),
                "tipo": "ALERTA_CORREDOR_RIESGO" if es_alerta else "snapshot_corredor",
                "alta_siniestralidad": corredor in riesgo,
                "umbral_por_minuto": UMBRAL,
                "emitido_en": datetime.utcnow(),
            }
            try:
                coll.insert_one(doc)
                if es_alerta:
                    print(
                        f"  🚦 ALERTA  corredor={corredor:<25}  "
                        f"vehs/min={por_minuto:.1f}  ventana={vent:%H:%M}",
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
