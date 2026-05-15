# ADR 02 · Arquitectura Lambda vs Kappa

**Estado:** Aceptado · _revisado 2026-05-15 para incorporar Flink real_
**Fecha:** 2026-05-12 (decisión original) · 2026-05-15 (revisión Sprint 6 cumplimiento rúbrica)
**Decisores:** equipo Pulso Medellín (ST1630, EAFIT)
**Módulo del curso:** 02 — Arquitecturas Lambda y Kappa

> **Nota 2026-05-15 (Sprint 6 — cumplimiento rúbrica § 3.1 y § 4.4):** la
> rúbrica oficial del proyecto (`docs/Proyecto_Final_ST1630.pdf`) exige
> **Apache Flink** explícitamente como motor streaming. Sin invalidar la
> decisión Lambda de abajo, se añadió un cluster Flink real (JobManager +
> TaskManager) con un job PyFlink (`src/streaming/flink_real/siata_alert_flink.py`)
> que reproduce la lógica del consumidor Python equivalente
> (`src/streaming/flink_jobs/siata_alert_job.py`) — mismo tópico, misma
> ventana tumbling, mismo sink Mongo, **pero con checkpointing nativo
> at-least-once** y paralelismo distribuido. Los jobs Python se mantienen
> como camino redundante porque ya cubren las preguntas S-1..S-4 sin
> overhead de cluster, y materializan el "lado streaming" del patrón Lambda.

## Contexto

Pulso Medellín integra 6 fuentes de movilidad del Valle de Aburrá con SLAs de
respuesta heterogéneos. Identificamos tres clases de consultas mutuamente
incompatibles bajo un único paradigma de procesamiento:

| Clase | Ejemplos | Latencia tolerable | Cardinalidad | Volumen procesado |
|-------|----------|---------------------|----------------|--------------------|
| **Analítica histórica** | B-1..B-4 (correlaciones multi-año, severidad por comuna) | minutos–horas | millones de filas × años | 100 % del histórico, joins multi-fuente |
| **Operacional reciente** | S-1 EnCicla, S-3 SIMM, S-4 Metro RT (dashboard cada 5 s) | segundos | últimos 5–10 min | flujo de eventos del minuto |
| **Híbrida (4.3 propuesta)** | alerta cuando afluencia RT cae bajo p90 histórico durante lluvia | segundos para emitir, pero **necesita** referencia batch | últimos 5 min + percentiles precomputados | stream + tabla cacheada |

La pregunta es si construimos:

- **Una sola pipeline** que procese todo en streaming (Kappa), reprocesando
  la historia desde Kafka cuando hace falta, o
- **Dos pipelines paralelas** — batch (Bronze→Silver→Gold sobre Iceberg) y
  streaming (Kafka→Python jobs→MongoDB) — sincronizadas en una tercera capa
  servidora (Lambda).

## Decisión

**Adoptamos Lambda.** El batch corre sobre Iceberg/MinIO (Bronze→Silver→Gold)
para las preguntas B-1..B-4 y la generación de referencias (percentiles
históricos, corredores de alta siniestralidad). El streaming corre sobre
Kafka + jobs Python sobre `stream-runner` para las preguntas S-1..S-4. La
capa servidora es **MongoDB** (vista operacional) más los **archivos JSON
de referencia** que el job híbrido lee al arranque y que provienen del lado
batch (`data/processed/percentiles_metro.json`,
`corredores_alta_siniestralidad.json`).

## Alternativas evaluadas

### A. Kappa puro (streaming-only)

Todas las preguntas se resuelven con un único pipeline streaming. Kafka es la
fuente canónica y los reprocesos batch se simulan con replay desde
`auto_offset_reset="earliest"` y retención larga (>1 año).

- **Pros**
  - Una sola codebase, un solo paradigma mental.
  - Sin desincronización entre vistas batch y streaming.
  - Replay para corregir errores se vuelve barato si los tópicos retienen lo suficiente.
- **Contras**
  - Requiere **retener años en Kafka** (270k incidentes MEData + 1.7M lecturas
    SIATA + 240k afluencia Metro → varios GB). Costo operativo alto.
  - Los joins entre 6 fuentes con ventanas largas (un mes para B-1) son
    expresables en Flink SQL pero **fragiles**: una pausa del consumer y
    el estado distribuido se desbalancea.
  - El equipo no tiene rodaje en estado distribuido Flink. Para 1 sprint
    con 4 jobs simples ya es suficiente; agregar joins inter-stream sobre
    años de datos rompe el budget.

### B. Lambda (la elegida)

Batch sobre Iceberg, streaming sobre Kafka + Python jobs, servicio sobre
MongoDB + JSONs derivados.

- **Pros**
  - Cada lado usa la herramienta correcta: Spark+Iceberg para joins masivos
    y time-travel, Kafka+Python para latencia segundo-a-segundo.
  - El job híbrido (`job_hibrido.py`) **demuestra explícitamente** el patrón
    Lambda: lee del lado batch (referencias percentiles) y combina con
    streaming (afluencia 5min real) para emitir una alerta única.
  - El lado batch tolera caída del stream sin perder histórico; el lado
    streaming tolera caída de Iceberg sin perder operación reciente.
- **Contras**
  - **Doble codebase**: las definiciones de "alta siniestralidad" o
    "percentil 90 de afluencia" pueden divergir si no se centralizan
    (decisión correlacionada: viven en `scripts/exportar_referencias_streaming.py`
    como único punto de generación).
  - Latencia de propagación batch → streaming: las referencias se
    regeneran manualmente (no en cada ingestión). Aceptable porque MEData
    y Metro publican mensual/anualmente, no minuto-a-minuto.

### C. Híbrido sin Iceberg (sólo Mongo + Kafka)

Streaming a Mongo (real-time) y Mongo como almacén histórico también
(agregaciones nocturnas con `aggregate()`).

- **Pros**: stack único, operativamente simple.
- **Contras**: Mongo no es columnar — las consultas analíticas tipo B-1
  (correlación entre PM2.5 mensual y afluencia mensual a lo largo de 3 años)
  son órdenes de magnitud más lentas que en Parquet+Iceberg. Pierde el
  argumento de "interoperabilidad multi-motor" del lakehouse (módulo 05).

## Mapeo a los 3 SLAs del proyecto

| SLA | Implementación elegida | Razón |
|-----|-------------------------|-------|
| B-1..B-4 (analítica) | Batch Iceberg, tablas Gold, notebook EDA | Latencia tolerable de horas; necesita joins inter-fuente, percentiles, partition pruning por año |
| S-1..S-4 (operacional) | Streaming Kafka → Python jobs → Mongo | Necesita ventana de minutos; consumidor (dashboard) refresca cada 5 s |
| Híbrida 4.3 | `job_hibrido.py` lee JSON de referencia + 2 tópicos Kafka | Materializa explícitamente el patrón Lambda |

## Consecuencias

### Lo que se vuelve más fácil

- Migrar el lado batch a otro motor (Trino, DuckDB, Snowflake) sin tocar el
  streaming: el contrato es la tabla Iceberg con su namespace `pulsomed.gold.*`.
- Reemplazar `data/processed/percentiles_metro.json` por consulta Iceberg en
  vivo (PyIceberg) es local al `job_hibrido.py` — no afecta a los productores
  ni a los 4 jobs de ventana. Esta migración se hace en Sprint 4.
- Agregar una 7ª pregunta operacional sólo requiere un productor + un job +
  una colección Mongo. No toca al lado batch.

### Lo que se vuelve más difícil

- Mantener **consistencia semántica** entre las dos pipelines. Ejemplo
  concreto: "alta siniestralidad" se define una vez en el script de
  exportación de referencias (`scripts/exportar_referencias_streaming.py`)
  y se consume tanto en batch (`build_all.py` ranking) como en streaming
  (`simm_aforo_job.py` lista de corredores). Si se ajusta la fórmula OMS-like
  hay que regenerar el JSON.
- Operacionalmente son **dos pipelines a monitorear**: el batch via Spark UI,
  el streaming via `docker compose logs stream-runner`.

### Señales que indicarían revisar esta decisión

1. Si el dashboard empieza a depender de consultas analíticas (joins
   multi-fuente sobre semanas) que ya no caben en ventanas de minutos en
   Mongo, la respuesta correcta es **no** colar Spark al lado streaming,
   sino que el dashboard consulte directo a Iceberg vía Trino (Bonus 1 del
   Sprint 5).
2. Si el volumen Kafka histórico se vuelve manejable (compresión, infra
   cloud con tópicos compactos), una migración a Kappa con replay se vuelve
   viable. Hoy con un Docker local en máquina del equipo, no.

## Implementación

- Batch path: `src/batch/{bronze,silver,gold}/`, ejecutado por `make pipeline-batch`.
- Streaming path: `src/streaming/{producers,flink_jobs}/`, levantado por
  `make pipeline-streaming-completo` y los 9 procesos paralelos descritos
  en `docs/sprints/sprint-3-streaming-completo.md`.
- Materialización del patrón Lambda en código:
  `src/streaming/flink_jobs/job_hibrido.py` (sección 4.3 de la propuesta).
- Capa servidora:
  - Mongo `pulsomed.{alertas_aire, disponibilidad_encicla, aforos_corredor,
    afluencia_metro_rt, alertas_hibridas}`.
  - Iceberg `demo.pulsomed.gold.*` (consultable por notebooks, próximamente
    Trino).

## ADRs relacionados

- ADR 04 (`04-benchmark-formatos.md`) — qué formato se elige en el lado batch.
- ADR 05 (`05-delta-vs-iceberg.md`) — qué tabla format se elige para Gold.
- ADR 07 (`07-cloud-aws-vs-gcp.md`) — dónde corre todo en producción
  (Sprint 5).
