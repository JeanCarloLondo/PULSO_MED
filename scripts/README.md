# `scripts/` — Auxiliares de operación

Scripts cortos que **no son parte del pipeline** pero ayudan al equipo.

| Archivo | Sprint | Para qué |
|---------|--------|----------|
| `download_datasets.sh` | 1 | Descargar fuentes públicas a `data/raw/` |
| `init_iceberg_namespaces.py` | 1 | Crear `pulsomed.bronze`, `pulsomed.silver`, `pulsomed.gold` |
| `consultar_alertas.py` | 3 | CLI para consultar la colección Mongo de alertas RT |
| `seed_kafka_topics.sh` | 2 | Crear tópicos Kafka con la retención correcta |

Esta carpeta arranca vacía en Sprint 0; se llena en Sprints 1+.
