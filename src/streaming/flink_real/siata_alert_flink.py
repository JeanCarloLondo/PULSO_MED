"""
siata_alert_flink.py — Job **PyFlink real** que cubre la rúbrica § 4.4.

Implementación del mismo caso de uso del Sprint 2 (alerta PM2.5 por zona),
pero ahora corriendo en un cluster Apache Flink real (JobManager + TaskManager
en Docker) en vez del consumidor Python sobre `stream-runner`.

Cumple punto por punto la rúbrica § 4.4:
  · § 4.4.1 — KafkaSource conectado al tópico `siata.lecturas`
  · § 4.4.2 — Tumbling window de N minutos sobre la clave `zona`
  · § 4.4.3 — Sink hacia MongoDB (colección `alertas_aire_flink`)
  · § 4.4.4 — Checkpointing cada 60 s con semántica AT_LEAST_ONCE

El job Python original (`src/streaming/flink_jobs/siata_alert_job.py`) se
mantiene en producción como camino redundante (ADR 02 Lambda); este job es la
implementación canónica para evaluación del módulo Flink.

Cómo correr:
    docker compose up -d flink-jobmanager flink-taskmanager
    docker compose exec flink-jobmanager flink run \\
        --python /workspace/src/streaming/flink_real/siata_alert_flink.py
    # equivalente: make flink-submit-alert

Variables de entorno (leídas del JobManager/TaskManager):
    KAFKA_BOOTSTRAP_SERVERS   default kafka:9092
    MONGO_HOST                default mongodb
    MONGO_INITDB_ROOT_*       credenciales Mongo
    VENTANA_MINUTOS           default 1   (corto para demo)
    UMBRAL_PM25               default 75.0
    PARALELISMO               default 2   (= particiones del tópico)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Iterable
from urllib.parse import quote_plus

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SIATA = "siata.lecturas"
GROUP_ID = "pulsomed-alert-flink"

VENTANA_MIN = int(os.getenv("VENTANA_MINUTOS", "1"))
UMBRAL_PM25 = float(os.getenv("UMBRAL_PM25", "75.0"))
PARALELISMO = int(os.getenv("PARALELISMO", "2"))

MONGO_USER = quote_plus(os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin"))
MONGO_PASS = quote_plus(os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin12345"))
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/pulsomed?authSource=admin"
)
COL_ALERTAS = "alertas_aire_flink"


# ── Parsing y mapeo --------------------------------------------------------------

def _parsear_lectura(raw: str) -> tuple[str, float] | None:
    """Convierte el JSON crudo del tópico a (zona, pm25). Filtra inválidos."""
    try:
        d = json.loads(raw)
        zona = (d.get("zona") or "").strip()
        pm = d.get("pm25")
        if not zona or pm is None:
            return None
        return zona, float(pm)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


# ── Función de ventana (agrega y decide si emite alerta) -------------------------

class AgregadorVentana(ProcessWindowFunction):
    """Promedia PM2.5 por zona y deja pasar sólo las que superan el umbral."""

    def process(
        self,
        zona: str,
        context: ProcessWindowFunction.Context,
        elementos: Iterable[tuple[str, float]],
    ):
        valores = [v for _, v in elementos]
        if not valores:
            return
        pm_avg = sum(valores) / len(valores)
        if pm_avg < UMBRAL_PM25:
            return
        ventana = context.window()
        yield {
            "zona": zona,
            "ventana_inicio": datetime.utcfromtimestamp(ventana.start / 1000).isoformat(),
            "ventana_fin": datetime.utcfromtimestamp(ventana.end / 1000).isoformat(),
            "pm25_avg": round(pm_avg, 2),
            "lecturas": len(valores),
            "umbral": UMBRAL_PM25,
            "gravedad": "moderada" if pm_avg < 100 else "alta",
            "motor": "flink",
            "emitido_en": datetime.utcnow().isoformat(),
        }


# ── Sink hacia MongoDB ----------------------------------------------------------
# PyFlink no trae sink Mongo nativo en 1.18 para DataStream API; lo
# implementamos con `MapFunction`-style usando pymongo. La conexión se
# materializa en cada TaskManager la primera vez que se procesa un evento.

def _escribir_mongo(value):
    """Escribe la alerta en MongoDB; compatible con PyFlink map (no usa add_sink)."""
    try:
        from pymongo import MongoClient
        cli = MongoClient(MONGO_URI)
        coll = cli["pulsomed"][COL_ALERTAS]
        try:
            coll.create_index([("zona", 1), ("ventana_inicio", 1)], unique=True)
        except Exception:
            pass
        coll.insert_one(dict(value))
        cli.close()
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate" not in msg and "e11000" not in msg:
            print(f"  ⚠ Mongo: {exc}", flush=True)
    return value


# ── Construcción del pipeline ---------------------------------------------------

def _construir_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALELISMO)
    # Checkpointing — exigido por rúbrica § 4.4.4
    env.enable_checkpointing(60_000, CheckpointingMode.AT_LEAST_ONCE)
    cfg = env.get_checkpoint_config()
    cfg.set_min_pause_between_checkpoints(30_000)
    cfg.set_checkpoint_timeout(120_000)
    cfg.set_max_concurrent_checkpoints(1)
    return env


def main() -> None:
    print(
        f"→ PyFlink siata_alert_flink  ventana={VENTANA_MIN}min  umbral={UMBRAL_PM25}  paralelismo={PARALELISMO}",
        flush=True,
    )

    env = _construir_env()

    fuente = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_SIATA)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    flujo = (
        env.from_source(
            source=fuente,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="siata-kafka-source",
        )
        .map(_parsear_lectura, output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .window(TumblingProcessingTimeWindows.of(Time.minutes(VENTANA_MIN)))
        .process(
            AgregadorVentana(),
            output_type=Types.MAP(Types.STRING(), Types.STRING()),
        )
    )

    flujo.map(_escribir_mongo, output_type=Types.MAP(Types.STRING(), Types.STRING())).print()

    env.execute("pulsomed-siata-alert-flink")


if __name__ == "__main__":
    main()
