"""
init_kafka_topics.py — Crea los 4 tópicos del proyecto con la configuración
exigida por la rúbrica § 4.3.2: ≥2 particiones, retención configurada,
replicación coherente con el cluster.

Tópicos creados:
    siata.lecturas              · S-2 + Job híbrido (PM2.5/PM10)
    encicla.disponibilidad      · S-1 (estaciones EnCicla)
    metro.validaciones          · S-4 + Job híbrido (afluencia Metro)
    simm.aforos                 · S-3 (aforos vehiculares)

Cada tópico se crea con:
    num_partitions      = KAFKA_PARTITIONS         (default 2)
    retention.ms        = KAFKA_RETENTION_MS       (default 7 días)
    replication_factor  = 1                        (Kafka standalone single-node)
    cleanup.policy      = delete

Idempotente: si un tópico ya existe con otra configuración, el script lo
detecta y aplica `incrementalAlterConfigs` para sincronizar la retención
sin perder datos.

Ejecutar:
    docker compose exec -T stream-runner python /workspace/scripts/init_kafka_topics.py
    # o vía Make:  make init-kafka-topics
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/src")

try:
    from kafka.admin import (
        ConfigResource,
        ConfigResourceType,
        KafkaAdminClient,
        NewTopic,
    )
    from kafka.errors import TopicAlreadyExistsError
except ImportError:
    print("ERROR: pip install kafka-python", flush=True)
    sys.exit(1)

from shared.config import (
    KAFKA_BOOTSTRAP,
    KAFKA_PARTITIONS,
    KAFKA_RETENTION_MS,
    TOPIC_ENCICLA,
    TOPIC_METRO,
    TOPIC_SIATA,
    TOPIC_SIMM,
)

REPLICATION_FACTOR = 1  # cluster single-broker en docker-compose
TOPICOS = [TOPIC_SIATA, TOPIC_ENCICLA, TOPIC_METRO, TOPIC_SIMM]


def _config_topico() -> dict[str, str]:
    return {
        "retention.ms": str(KAFKA_RETENTION_MS),
        "cleanup.policy": "delete",
        "compression.type": "producer",
    }


def crear_o_actualizar(admin: KafkaAdminClient, nombre: str) -> str:
    """Devuelve 'creado' o 'actualizado' según corresponda."""
    nuevo = NewTopic(
        name=nombre,
        num_partitions=KAFKA_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        topic_configs=_config_topico(),
    )
    try:
        admin.create_topics([nuevo], validate_only=False)
        return "creado"
    except TopicAlreadyExistsError:
        # Sincronizar la retención del tópico existente
        recurso = ConfigResource(ConfigResourceType.TOPIC, nombre, _config_topico())
        admin.alter_configs([recurso])
        return "ya existía — config sincronizada"


def main() -> int:
    print(f"→ Conectando al broker Kafka en {KAFKA_BOOTSTRAP}…", flush=True)
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="pulsomed-init-topics",
        request_timeout_ms=10000,
    )

    print(
        f"→ Configuración común:  particiones={KAFKA_PARTITIONS}  "
        f"retention={KAFKA_RETENTION_MS // 1000 // 3600}h  "
        f"replicación={REPLICATION_FACTOR}",
        flush=True,
    )

    fallos = 0
    for topico in TOPICOS:
        try:
            estado = crear_o_actualizar(admin, topico)
            print(f"  ✓ {topico:<30s} {estado}", flush=True)
        except Exception as exc:
            print(f"  ✗ {topico:<30s} ERROR: {exc}", flush=True)
            fallos += 1

    # Verificación: listar tópicos y mostrar particiones
    print("\n→ Estado final:", flush=True)
    metadatos = admin.describe_topics(TOPICOS)
    for meta in metadatos:
        nombre = meta.get("topic")
        particiones = len(meta.get("partitions", []))
        print(f"  · {nombre:<30s} {particiones} partición(es)", flush=True)

    admin.close()
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
