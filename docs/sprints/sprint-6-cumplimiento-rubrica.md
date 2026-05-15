# Sprint 6 · Cumplimiento de la rúbrica oficial

> **Estado:** ✅ cerrado en código y documentación · 2026-05-15
> **Fuente:** `docs/Proyecto_Final_ST1630.pdf` (rúbrica oficial 2026)
> **Objetivo:** cerrar los gaps detectados al cruzar el proyecto entregado en
> Sprints 0–5 con los criterios de evaluación oficiales.

---

## Auditoría vs rúbrica

Antes de este sprint el proyecto tenía 4 gaps verificables contra la rúbrica:

| § Rúbrica | Pts | Gap | Cerrado por |
|-----------|-----|-----|-------------|
| 3.1 + 4.4 — **Apache Flink** | 15 | Streaming corría en Python en `stream-runner` | Cluster Flink real + job PyFlink |
| 4.3.2 — Tópicos Kafka ≥ 2 particiones + retención | 4 | Auto-create con defaults (1 partición) | Script `init_kafka_topics.py` |
| 4.6.4 + 3.1 — Iceberg features + 2 lotes Bronze | 7 | Time Travel parcial, Schema Evolution no demostrado | Script `demo_iceberg_features.py` |
| 4.8.2 — Sin hardcodes | 1 | `iceberg.properties` traía `admin/admin12345` | Volvió a `${ENV:VAR}` |
| Bonus § 3.2 | +1 | Faltaba Spark Structured Streaming | Job `siata_a_iceberg_streaming.py` |

Recuperación estimada: **~27 puntos** de los 100 + 1 de bonus.

---

## Lo que entregué

### 1. Apache Flink real (rúbrica § 3.1 + § 4.4)

Cluster JobManager + TaskManager basado en una imagen custom que extiende
`flink:1.18.1-scala_2.12-java11` con:

- Python 3 + `apache-flink==1.18.1` (PyFlink)
- `pymongo` (sink hacia MongoDB desde un `SinkFunction` Python)
- `flink-sql-connector-kafka-3.0.2-1.18.jar` descargado a `/opt/flink/lib/`

```
docker/flink/Dockerfile                                imagen pulsomed/flink
src/streaming/flink_real/__init__.py
src/streaming/flink_real/siata_alert_flink.py          job PyFlink real
```

El job `siata_alert_flink.py` implementa:

- **§ 4.4.1 Consumo Kafka:** `KafkaSource` builder con `SimpleStringSchema`,
  `KafkaOffsetsInitializer.latest()` y group id `pulsomed-alert-flink`.
- **§ 4.4.2 Ventana con estado:** `key_by(zona).window(TumblingProcessingTimeWindows.of(N min))`
  con `ProcessWindowFunction` propia que agrega PM2.5 promedio.
- **§ 4.4.3 Sink NoSQL:** `MongoSink` (subclase de `SinkFunction`) que abre
  conexión pymongo lazy y crea índice único `(zona, ventana_inicio)`.
- **§ 4.4.4 Checkpointing:** `env.enable_checkpointing(60_000, AT_LEAST_ONCE)`
  con `min_pause_between_checkpoints=30s`, `timeout=120s`,
  `max_concurrent_checkpoints=1`. Backend `hashmap`, checkpoints persistidos
  en volumen `flink-checkpoints`.

**Cómo correr:**

```bash
make flink-up                  # construye imagen + levanta JobManager + TaskManager
make flink-submit-alert        # somete el job al cluster

# UI:                http://localhost:8082
# Alertas en Mongo:  db.alertas_aire_flink
```

**Decisión:** mantenemos también el job Python (`siata_alert_job.py`)
porque ya cubre las 4 preguntas operacionales sin overhead de cluster y
materializa el "lado streaming" del patrón Lambda. El job Flink es la
implementación canónica para evaluación del módulo y demuestra el
patrón con estado distribuido + checkpointing.

### 2. Tópicos Kafka configurados (rúbrica § 4.3.2)

```
scripts/init_kafka_topics.py
```

Usa `KafkaAdminClient` para crear los 4 tópicos del proyecto con:

```
num_partitions     = 2
replication_factor = 1     (single-broker en docker-compose)
retention.ms       = 7 días
cleanup.policy     = delete
compression.type   = producer
```

Idempotente: si el tópico ya existe, sincroniza la configuración con
`alter_configs`. La verificación final lista cada tópico con su número
real de particiones.

```bash
make init-kafka-topics
```

### 3. Demo de features Iceberg + 2 lotes Bronze (rúbrica § 3.1 + § 4.6.4)

```
scripts/demo_iceberg_features.py
```

Crea `demo.pulsomed.bronze._features_demo` y ejecuta cinco verificaciones:

1. **Lote 1 (ACID):** `CREATE TABLE` + `INSERT` de 3 filas → snapshot 1.
2. **Lote 2:** otro `INSERT` de 3 filas → snapshot 2 distinto + 6 filas acumuladas.
3. **Time Travel:** `spark.read.option("snapshot-id", snap1).table(...)` ve
   sólo 3 filas (las del primer lote) → demuestra historial sin pérdida.
4. **Schema Evolution:** `ALTER TABLE ADD COLUMN observacion STRING` + lote 3.
   Las filas viejas retornan `NULL`, las nuevas leen el valor → sin reescritura.
5. **DROP TABLE PURGE** para no contaminar el catálogo.

Retorna 0 si los 3 features pasan. Hace `SELECT * FROM tabla.snapshots`
para mostrar `snapshot_id`, `committed_at`, `operation` y `added_records`
de los 3 snapshots.

```bash
make iceberg-features
```

### 4. Spark Structured Streaming (bonus § 3.2)

```
src/streaming/structured/__init__.py
src/streaming/structured/siata_a_iceberg_streaming.py
```

Lee el tópico `siata.lecturas` con `spark.readStream.format("kafka")`,
parsea el JSON con el esquema definido en `ESQUEMA_SIATA`, y escribe a
`demo.pulsomed.bronze.siata_streaming` con micro-batches cada 30 s. El
checkpoint vive en `s3://warehouse/_checkpoints/siata_streaming`.

Convive con el job Flink: ambos consumen el mismo tópico pero escriben
a destinos distintos (Flink → MongoDB operacional, Spark SS → Iceberg
analítico). Demuestra que el mismo flujo de eventos alimenta a ambos
lados del lakehouse híbrido sin acoplamiento.

```bash
make stream-structured-siata
```

### 5. Trino sin hardcodes (rúbrica § 4.8.2)

`docker/trino/etc/catalog/iceberg.properties` vuelve a usar `${ENV:VAR}`:

```properties
s3.aws-access-key=${ENV:MINIO_ROOT_USER}
s3.aws-secret-key=${ENV:MINIO_ROOT_PASSWORD}
```

Las variables ya se inyectaban al contenedor desde `docker-compose.yml::trino.environment`,
así que el cambio es transparente. Trino 326+ resuelve `${ENV:VAR}` nativamente.

---

## Comandos nuevos

```bash
# Sprint 6 — cumplimiento rúbrica
make init-kafka-topics         # tópicos con 2 particiones + retención
make iceberg-features          # demo ACID + Time Travel + Schema Evolution
make flink-up                  # construir imagen + levantar JobManager + TaskManager
make flink-submit-alert        # someter job PyFlink siata_alert
make flink-ui                  # URL del JobManager UI
make stream-structured-siata   # bonus: Spark Structured Streaming
make cumplimiento-rubrica      # init-kafka-topics + iceberg-features + flink-up
```

---

## Mapa final · proyecto vs rúbrica

| § Rúbrica | Pts | Cubierto por |
|-----------|-----|--------------|
| 4.1 Descripción del problema | 18 | `docs/Propuesta_pulsomed_SID.pdf` + `docs/01-arquitectura.md` |
| 4.2 Arquitectura Docker | 12 | `docker-compose.yml` + diagrama Mermaid + README |
| 4.3 Kafka | 12 | 4 productores + `init_kafka_topics.py` (2 particiones + retención) |
| 4.4 Flink | 15 | `flink_real/siata_alert_flink.py` (DataStream + checkpointing) |
| 4.5 NoSQL | 12 | 5 colecciones Mongo + CLI + dashboard |
| 4.6 Spark + Iceberg | 15 | Bronze→Silver→Gold + `demo_iceberg_features.py` |
| 4.7 Integración | 10 | `job_hibrido.py` (lambda explícito) + 3 notebooks Gold |
| 4.8 Calidad | 5 | README actualizado + `iceberg.properties` sin hardcodes |
| 4.9 Demo | 8 | `make all` end-to-end + `make cumplimiento-rubrica` |
| **Bonus** | +5 | Trino (+2) · Makefile orquestado (+1) · Notebooks (+1) · Structured Streaming (+1) |

---

## Decisiones técnicas (las grandes)

**1. Mantener el `stream-runner` Python además del cluster Flink.**
Los 4 jobs Python ya cubren S-1..S-4 con bajo overhead. Migrar todos a
Flink real duplicaría el trabajo sin valor adicional para la evaluación.
La rúbrica § 4.4 pide **un** job Flink con las 4 características
(consumo Kafka, ventana con estado, sink NoSQL, checkpointing), no
exige que todo el streaming sea Flink. El job PyFlink real cubre el módulo,
los jobs Python cubren las preguntas de negocio.

**2. PyFlink DataStream API + sink Mongo en Python.**
PyFlink 1.18 no tiene connector Mongo nativo para DataStream API. Las
alternativas eran: (a) usar Table API + connector SQL (más restrictivo
para ventanas con estado custom), (b) implementar Sink en Java/Scala
(rompe el principio del proyecto en español), o (c) escribir un
`SinkFunction` Python con pymongo. Elegimos (c) porque mantiene la
codebase uniforme y el costo de overhead es bajo (1 conexión Mongo por
task slot, reutilizada).

**3. Checkpointing en `hashmap` backend con almacenamiento local.**
Suficiente para demo y para un cluster de un solo TaskManager. En
producción AWS (ADR 07) migraría a `rocksdb` con state en S3.

**4. Spark Structured Streaming escribe a Bronze, no a Silver/Gold.**
El bonus pide "alternativa o complemento a Flink". Escribir a Bronze
demuestra el patrón de ingesta streaming en Iceberg sin pisar las
tablas Bronze que produce el batch (que viven en `bronze.siata_lecturas`).
El destino es `bronze.siata_streaming` — separado, idempotente, con
columnas de Kafka metadata (`kafka_offset`, `kafka_partition`).

---

## Archivos nuevos en este sprint

```
docker/flink/Dockerfile                                       imagen Flink + PyFlink
docker-compose.yml                                            +2 servicios Flink
Makefile                                                      +9 targets (Flink, Kafka, Iceberg, SS)
scripts/init_kafka_topics.py                                  tópicos con 2 particiones + retención
scripts/demo_iceberg_features.py                              ACID + Time Travel + Schema Evolution
src/streaming/flink_real/__init__.py
src/streaming/flink_real/siata_alert_flink.py                 job PyFlink real
src/streaming/structured/__init__.py
src/streaming/structured/siata_a_iceberg_streaming.py         bonus Structured Streaming
docker/trino/etc/catalog/iceberg.properties                   credenciales vía ${ENV:...}
docs/decisiones/02-lambda-vs-kappa.md                         nota de revisión 2026-05-15
docs/sprints/sprint-6-cumplimiento-rubrica.md                 este archivo
```
