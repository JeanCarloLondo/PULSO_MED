# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project context

Pulso Medellín is a course project (EAFIT, ST1630 — Sistemas Intensivos en Datos) that integrates six public mobility data sources for the Aburrá Valley into a hybrid platform: a **Lakehouse Medallion** (Bronze → Silver → Gold over Apache Iceberg + MinIO) for analytical batch questions, plus a **streaming path** (Kafka + Flink + MongoDB, planned for Sprint 2+) for operational real-time questions.

Work is organized in **6 sprints + 1 cumplimiento**, tracked in `docs/00-roadmap.md`. As of last commit **Sprints 0-5 are closed** (rescate de datos reales en 1.5, ADRs + MapReduce legacy + refactor batch + job híbrido PyIceberg en 4, MLlib + grafo Metro + ADR Cloud 07 + bonuses Trino/`make all`/notebook EDA cruzado en 5), y un **Sprint 6 de cumplimiento de rúbrica** (`docs/sprints/sprint-6-cumplimiento-rubrica.md`) añadió Apache Flink real con job PyFlink + checkpointing, script de tópicos Kafka con ≥2 particiones + retención, demo verificable de ACID/Time Travel/Schema Evolution con 2 lotes Bronze, y un job Spark Structured Streaming como bonus. El proyecto se ejecuta end-to-end con `make all` y se valida contra la rúbrica con `make cumplimiento-rubrica`.

## Common commands

Everything is driven through `make` (which wraps `docker compose`). The full stack runs in containers — never run PySpark or smoke tests directly on the host.

```bash
make up                  # start MinIO, Iceberg REST, Spark+Jupyter, MongoDB
make down                # stop (preserves volumes)
make clean               # stop AND drop volumes (destructive — prompts y/N)
make ps / make logs      # status / logs (logs SERVICE=<name> for one service)
make smoke               # full Sprint 0 health check (MinIO + Iceberg + Mongo)
make smoke-iceberg       # run a single smoke test
make shell / make pyspark  # bash or pyspark inside the spark-iceberg container
```

Sprint 1 batch pipeline:

```bash
make download-data       # bash scripts/download_datasets.sh — pulls 6 datasets to data/raw/
make init-namespaces     # one-time: create pulsomed.{bronze,silver,gold} in Iceberg
make ingest-bronze-all   # all 6 sources → Bronze (or one at a time: ingest-bronze-medata, etc.)
make transform-silver    # Bronze → Silver
make build-gold          # Silver → Gold
make pipeline-batch      # everything above end-to-end
```

Sprint 2 streaming:

```bash
make stream-up                                              # Zookeeper + Kafka + stream-runner
make stream-alert-job VENTANA_MINUTOS=1 UMBRAL_PM25=75      # consumidor + ventana
make stream-producer INTERVALO_S=0.05 INYECTAR_PICO_CADA=5  # productor con picos
make stream-alertas ULTIMAS=10min                           # CLI a Mongo
```

Sprint 1.5 datos reales (idempotente — sólo descarga lo que falta):

```bash
make datos-reales                # Metro xlsx (ArcGIS DCAT) + SIATA Dataverse + EnCicla OSM
make exportar-referencias        # JSONs derivados de los CSV reales (para Sprint 3 jobs)
```

Sprint 3 streaming completo (preguntas S-1..S-4 + job híbrido batch↔streaming):

```bash
make pipeline-streaming-completo                  # stack + referencias precomputadas

# productores (cada uno en su terminal)
make stream-encicla-producer / -job               # S-1, sliding 1m/30s
make stream-producer / stream-alert-job           # S-2, tumbling 10m (Sprint 2)
make stream-simm-producer  / stream-simm-job      # S-3, tumbling 5m
make stream-metro-producer / stream-metro-job     # S-4, tumbling 5m
make stream-hibrido                               # 4.3, batch↔streaming
make dashboard                                    # http://localhost:8501
```

Sprint 4 (legacy MapReduce + benchmark formatos):

```bash
make pipeline-batch              # ahora incluye gold.percentiles_metro (b5) y schemas reales
make pipeline-legacy             # MEData pre/post-2017 → mrjob → bronze.medata_incidentes_legacy_mr
make benchmark-formatos          # ADR 04 reproducible
make stream-hibrido              # ahora lee gold.percentiles_metro vía PyIceberg, con fallback al JSON
```

A `.env` file is **required** before `make up`. Copy `.env.example` and edit. The `Makefile`'s `env-check` target enforces this.

The Makefile uses bash (`SHELL := /bin/bash`) and `MSYS_NO_PATHCONV=1` for `docker compose exec` — needed on Git Bash / Windows so paths like `/workspace/...` aren't mangled. **On native Windows MSYS make**, the `docker compose` plugin lookup also fails because make doesn't propagate `USERPROFILE`/`PROGRAMFILES`/`APPDATA` to its subshell. The user runs everything via WSL, where this isn't an issue. If a future contributor hits "unknown command: docker compose" from `make`, run the underlying `docker compose exec ...` directly or use WSL.

## Architecture (read this before editing)

### Container topology

All services live on the `pulsomed-net` Docker network. **Service names are hostnames**: inside the network you reach MinIO at `http://minio:9000`, the catalog at `http://iceberg-rest:8181`, Mongo at `mongodb:27017`. From the host, the same services are mapped to localhost ports listed in `Makefile`'s `up` target.

`spark-iceberg` (image `tabulario/spark-iceberg:latest`) is the workhorse — it has Spark 3.5, Iceberg jars, AWS S3 bundle, and Jupyter pre-installed. The repo is bind-mounted into it:

| Host path | Container path |
|-----------|---------------|
| `./src` | `/workspace/src` |
| `./scripts` | `/workspace/scripts` |
| `./tests` | `/workspace/tests` |
| `./data` | `/workspace/data` |
| `./notebooks` | `/home/iceberg/notebooks` |

So scripts are invoked as `docker compose exec spark-iceberg python /workspace/scripts/foo.py` (the Makefile already does this).

`iceberg-rest` does **not** have a healthcheck — its image lacks `curl`/`wget`/`nc`, so HTTP probes always fail. `spark-iceberg` depends on it with `service_started`, and Python clients retry connections (see `scripts/init_iceberg_namespaces.py` for the canonical retry loop). Don't add a healthcheck to `iceberg-rest`.

### Lakehouse layout

Iceberg catalog name is **`demo`** (pre-wired by the `tabulario/spark-iceberg` image via env vars), not `pulsomed`. The `pulsomed` is a **namespace** under that catalog. Tables follow:

```
demo.pulsomed.bronze.<fuente>      # e.g. demo.pulsomed.bronze.medata_incidentes
demo.pulsomed.silver.<entidad>
demo.pulsomed.gold.<metrica>
```

Physical warehouse: `s3://warehouse/` on MinIO.

The Bronze → Silver → Gold dataflow (per source) is documented in `docs/01-arquitectura.md` (Mermaid diagram). Don't invent new namespaces or table-naming schemes — reuse the constants in `src/shared/config.py`.

### `src/shared/config.py` is the single source of truth

All catalog/REST/MinIO/Mongo strings, table names, Kafka topic names, and the Spark session factory live here. **Never hardcode** these in scripts — import from `shared.config`. The Spark session factory is `crear_spark_session(nombre_app)`.

Scripts running inside the container insert `/workspace/src` onto `sys.path` so the `shared` package resolves:

```python
import sys
sys.path.insert(0, "/workspace/src")
from shared.config import crear_spark_session, TBL_BRONZE_MEDATA
```

### MongoDB vs Gold (decision rule, from architecture doc)

Operational / "what's happening now" / cardinality < 10 min of history → MongoDB. Analytical / cross-source / multi-year → Gold (Iceberg). When designing new outputs, classify them against this rule first.

### Privacy: HMAC pseudonymization

EnCicla `id_usuario` must be HMAC-SHA256-pseudonymized **before** landing in Bronze (Ley 1581 compliance). The secret is `HMAC_USER_PSEUDO_SECRET` in `.env` and exposed only to the Spark container — it must never appear in notebooks, code, or commits. `src/shared/config.HMAC_SECRET` reads it from the environment.

## Team conventions (enforce in code review)

- **Language**: code, docstrings, and comments are in **Spanish** (matches the domain and proposal). Technical identifiers — Iceberg table names, Kafka topics, Mongo collections, Python module names — are in **English `snake_case`** (e.g. `bronze.medata_incidentes`, never `bronze.incidentes_medata_de_la_movilidad`).
- **Commits**: conventional commits in Spanish. Example: `feat(bronze): ingesta de incidentes MEData con append por lote`.
- **Branches**: `main` is always stable. Sprints on `sprint/N-nombre`, features on `feat/N-descripcion-corta`.
- **Docs-first**: a module isn't "done" without a `.md` (in `docs/` or co-located) explaining what it does, how to run it, and the technical decision it embodies. New big decisions go in `docs/decisiones/` as ADRs.
- **Sprint logs**: after closing a sprint, append a section to `docs/instructivo-maestro.md` summarizing what was delivered and why each decision was made.

## What lives where

- `src/batch/{bronze,silver,gold}/` — PySpark pipelines for Sprint 1: Bronze ingest (6 sources), Silver transforms (one `transform_all.py`), Gold aggregates (`build_all.py` for B-1..B-4).
- `src/shared/bronze_utils.py` — auditoría columns + `escribir_bronze` helper. Used by every Bronze script; don't reimplement.
- `src/shared/config.py` — single source of truth. **Pyspark is imported lazily inside `crear_spark_session`** so `from shared.config import ...` works from non-Spark containers (e.g. `stream-runner`). Don't add top-level pyspark imports here.
- `src/streaming/{producers,flink_jobs}/` — Sprint 2 + 3: SIATA, EnCicla, SIMM, Metro producers + tumbling/sliding window jobs (Python, NOT PyFlink — ADR cerrado en `docs/decisiones/02-lambda-vs-kappa.md`). Sprint 3 añade `flink_jobs/job_hibrido.py`; **Sprint 4** lo migró a PyIceberg en vivo (lee `gold.percentiles_metro` con fallback al JSON precomputado).
- `src/legacy/` — Sprint 4 (Módulo 01 arqueología): `generar_dataset_legacy.py` reconstruye la heterogeneidad pre/post-2017; `mapreduce_incidentes.py` es el job mrjob que normaliza; su salida la ingiere `src/batch/bronze/ingest_legacy_mr.py` a `bronze.medata_incidentes_legacy_mr`.
- `scripts/` — operational scripts: `download_datasets.sh`, `overpass_a_geojson.py` (GeoMedellín OSM), `generar_muestras_sinteticas.py` (synthetic fallback), `init_iceberg_namespaces.py`, `consultar_alertas.py`. **Sprint 1.5 reales:** `descargar_metro_afluencia_real.py`, `descargar_siata_real.py`, `descargar_encicla_estaciones.py`. **Sprint 3 referencias:** `exportar_referencias_streaming.py`. **Sprint 4:** `benchmark_formatos.py` (reproducible para ADR 04).
- `app/dashboard.py` — Streamlit (Sprint 3) con 5 paneles + mapa pydeck. Refresca cada 5 s.
- `data/processed/` — JSONs derivados (gitignored): `percentiles_metro.json` (de afluencia real), `corredores_alta_siniestralidad.json` (de MEData real). Generados por `make exportar-referencias`.
- `tests/smoke/` — Sprint 0 stack-health tests.
- `data/raw/` — raw datasets (gitignored). MEData and SIMM are real; Metro/SIATA/EnCicla are synthetic-by-default with the same schema as the real sources, so swapping in real data later requires no code changes.
- `notebooks/01_eda_gold.ipynb` — Sprint 1 EDA over the 4 Gold tables.
- `docs/sprints/` — one md per closed sprint (`sprint-0..sprint-4-legacy-y-adrs.md`). `docs/instructivo-maestro.md` is the changelog of decisions across all sprints.
- `docs/decisiones/` — ADRs. Cerrados en Sprint 4: `02-lambda-vs-kappa.md`, `04-benchmark-formatos.md`, `05-delta-vs-iceberg.md`. Pendiente Sprint 5: `07-cloud-aws-vs-gcp.md`.

### Schemas reales después del refactor Sprint 4

| Fuente | Esquema Bronze (real, post Sprint 4) |
|--------|--------------------------------------|
| `bronze.metro_afluencia` | `fecha, linea, hora, pasajeros` (formato largo, hora×línea — la fuente NO desglosa por estación) |
| `bronze.siata_lecturas` | `estacion_id, estacion_nombre, zona, latitud, longitud, municipio, timestamp, pm25, pm10` + cols meteorológicas NULL (pendiente Sprint 5) |
| `bronze.medata_incidentes_legacy_mr` | salida del job mrjob: `nro_radicado, fecha, anio, mes, clase, gravedad, barrio, comuna, direccion, longitud, latitud` |
| `gold.percentiles_metro` | `linea, franja_horaria, p50, p75, p90, p95, muestras` — leída en vivo por `job_hibrido.py` vía PyIceberg |

Si el job híbrido necesita PyIceberg desde `stream-runner`, las env vars
`AWS_ACCESS_KEY_ID/SECRET`, `ICEBERG_REST_URI` y `ICEBERG_S3_ENDPOINT` ya
están en `docker-compose.yml::stream-runner`. La librería se instala en el
boot command (`pyiceberg[s3fs,pyarrow]`).