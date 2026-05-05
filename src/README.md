# `src/` — Código del pipeline

Esta carpeta se llena progresivamente sprint a sprint:

| Subcarpeta | Sprint | Para qué |
|------------|--------|----------|
| `batch/bronze/` | 1 | Scripts de ingesta cruda (CSV/JSON/Geo → Iceberg Bronze) |
| `batch/silver/` | 1 | Limpieza, deduplicación, joins espaciales y temporales |
| `batch/gold/` | 1 | Agregaciones que responden las preguntas B-1..B-4 |
| `streaming/producers/` | 2 | Productores Kafka (Python) que simulan eventos SIATA, Metro, EnCicla, SIMM |
| `streaming/flink_jobs/` | 2-3 | Jobs Flink con ventanas temporales |
| `streaming/sinks/` | 2-3 | Lógica de escritura a MongoDB |
| `shared/` | transversal | Esquemas, configuraciones, utilidades comunes |

## Convenciones

- Cada job/script debe correrse desde el contenedor de Spark, no desde el host.
- Cada archivo `.py` empieza con un docstring que explica qué hace, qué entrada
  espera, y qué salida produce.
- Los scripts tienen un `if __name__ == "__main__":` con argparse para CLI.
- No hay magic strings: nombres de tablas y tópicos viven en `src/shared/config.py`.
