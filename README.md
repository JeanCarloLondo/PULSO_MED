# Pulso Medellín

> Plataforma de datos híbrida (batch + streaming) para la integración de la movilidad urbana del Valle de Aburrá.

**Curso:** ST1630 — Sistemas Intensivos en Datos · Universidad EAFIT
**Equipo:** Jean Carlo Londoño Ocampo · Moisés Vergara Garcés · Alejandro Garcés Ramírez
**Estado:** Sprints 0–5 cerrados · Pipeline completo ejecutable con un solo comando

---

## Tabla de contenido

1. [¿Qué es esto?](#qué-es-esto)
2. [Arquitectura en un vistazo](#arquitectura-en-un-vistazo)
3. [Prerrequisitos](#prerrequisitos)
4. [Ejecución guiada — la ruta corta (`make all`)](#ejecución-guiada--la-ruta-corta-make-all)
5. [Ejecución guiada — paso por paso](#ejecución-guiada--paso-por-paso)
6. [Demo streaming (Sprints 2 + 3)](#demo-streaming-sprints-2--3)
7. [Análisis y consumo de resultados](#análisis-y-consumo-de-resultados)
8. [Estructura del repositorio](#estructura-del-repositorio)
9. [Convenciones del equipo](#convenciones-del-equipo)
10. [Cobertura del curso](#cobertura-del-curso)
11. [Troubleshooting](#troubleshooting)
12. [Documentación detallada](#documentación-detallada)

---

## ¿Qué es esto?

Un sistema que integra **seis fuentes públicas de datos de movilidad** del Valle de Aburrá (MEData, SIMM, Metro de Medellín, EnCicla, SIATA, GeoMedellín) en una arquitectura **Lakehouse Medallion** (Bronze → Silver → Gold sobre Apache Iceberg + MinIO) combinada con un **camino streaming** (Kafka + jobs en Python + MongoDB) para responder simultáneamente:

- **Preguntas analíticas batch (B-1..B-4):** correlación PM2.5↔afluencia Metro, accidentalidad por comuna, demanda EnCicla vs clima, corredores de mayor riesgo compuesto.
- **Preguntas operacionales streaming (S-1..S-4):** disponibilidad EnCicla en vivo, alertas PM2.5, aforos SIMM, afluencia Metro RT.
- **Pregunta híbrida (sección 4.3 propuesta):** cuando llueve, ¿cae la afluencia Metro por debajo del p90 histórico para esa franja horaria? — leyendo Gold-Iceberg en vivo desde el stream.

La pregunta central que articula el proyecto:

> Cuando llueve fuerte en la zona nororiental de Medellín, ¿cuántos minutos de retraso se generan en el Metrocable K y cuántos usuarios migran a EnCicla o Metroplús como alternativa?

Detalle de dominio en [`docs/Propuesta_pulsomed_SID.pdf`](docs/Propuesta_pulsomed_SID.pdf).

---

## Arquitectura en un vistazo

```
   Fuentes públicas                  Lakehouse (Iceberg + MinIO)
   ────────────────                  ────────────────────────────
   ┌────────────┐  download_data    ┌─────────┐    ┌─────────┐    ┌──────┐
   │ MEData     │ ───────────────► │ Bronze  │ ─► │ Silver  │ ─► │ Gold │ ─► Notebooks
   │ SIMM       │                   │ (raw +  │    │ (clean+ │    │(B-1..│    Trino
   │ Metro      │                   │ audit)  │    │ joins)  │    │ B-4) │    Dashboard
   │ EnCicla    │                   └─────────┘    └─────────┘    └──┬───┘
   │ SIATA      │                                                    │
   │ GeoMedellín│ ────► Kafka ──► Jobs Python ──► MongoDB ──► Dashboard
   └────────────┘       (4 tópicos: S-1..S-4)    (alertas)         (Streamlit)
                                          ▲
                                          │  PyIceberg en vivo (job híbrido 4.3)
                                          └──── Gold.percentiles_metro
```

Stack completo: **MinIO + Iceberg REST Catalog + Spark 3.5 + Kafka + MongoDB + Trino + Streamlit**, todo orquestado por `docker-compose.yml`.

Diagrama detallado y decisiones de namespace en [`docs/01-arquitectura.md`](docs/01-arquitectura.md).

---

## Prerrequisitos

| Herramienta | Versión mínima | Cómo verificar |
|-------------|----------------|----------------|
| Docker Desktop (Windows/macOS) o Docker Engine + Compose v2 (Linux) | 24.x | `docker --version && docker compose version` |
| `make` (GNU Make) | 4.0 | `make --version` |
| `bash` | cualquiera | viene en WSL, Git Bash, macOS, Linux |
| `python3` (host, para descargas reales del Sprint 1.5) | 3.10+ | `python3 --version` |
| RAM libre | **8 GB mínimo** (16 GB recomendado para `make all`) | — |
| Disco libre | ~10 GB (imágenes Docker + datos crudos) | — |

> **Windows:** ejecutar todos los comandos `make` desde **WSL2** o Git Bash. El proyecto está probado en WSL. El Makefile usa `MSYS_NO_PATHCONV=1` y `SHELL := /bin/bash` para no romper paths como `/workspace/...`.

> **Conexión a internet** sólo es necesaria para `make download-data` y `make datos-reales`. El resto del pipeline corre 100% local.

---

## Ejecución guiada — la ruta corta (`make all`)

Si lo único que querés es ver el pipeline end-to-end de Sprints 0 a 5 funcionando:

```bash
# 1. Clonar el repo y entrar
git clone <url-del-repo> pulso-medellin
cd pulso-medellin

# 2. Configurar variables de entorno
cp .env.example .env
# (opcional) editar .env para cambiar contraseñas y la clave HMAC

# 3. Descargar datos reales (MEData, SIMM, Metro, SIATA, EnCicla, GeoMedellín)
make download-data        # ~5 min, idempotente
make datos-reales         # rescate Sprint 1.5: Metro xlsx + SIATA Dataverse + EnCicla OSM

# 4. Pipeline completo (Sprint 0 → 5) en un solo comando
make all
```

`make all` orquesta:

1. `up` — levanta MinIO, Iceberg REST, Spark, MongoDB.
2. `init-namespaces` — crea `pulsomed.bronze/silver/gold` en Iceberg.
3. `pipeline-batch` — ingesta a Bronze de las 6 fuentes → Silver → Gold (B-1..B-4 + percentiles Metro).
4. `pipeline-legacy` — genera CSV pre/post-2017 → job mrjob → Bronze legacy MR (Módulo 01).
5. `pipeline-sprint5` — entrena RandomForest (Módulo 06a) + PageRank/Dijkstra red Metro (Módulo 06b).

Al terminar, tendrás disponibles:

- **7 tablas Gold** en Iceberg consultables desde Spark, Jupyter, Trino.
- Modelo ML serializado en `data/processed/modelos/fatalidad_rf/`.
- Tablas `gold.red_metro_pagerank` y `gold.red_metro_rutas_optimas`.

> Tiempo estimado en máquina con 16 GB RAM / SSD: **~25–35 min** (la mayoría es ingesta Bronze y entrenamiento del bosque aleatorio).

---

## Ejecución guiada — paso por paso

Si querés entender qué hace cada etapa, corré los targets uno a uno. Cada uno es idempotente.

### Paso 1 — Levantar el stack (Sprint 0)

```bash
cp .env.example .env
make up                  # levanta MinIO, Iceberg REST, Spark, MongoDB
make ps                  # verifica que todos los contenedores estén Up
make smoke               # 3 smoke tests: MinIO + Iceberg + MongoDB
```

URLs útiles tras `make up`:

| Servicio | URL | Notas |
|----------|-----|-------|
| MinIO Console | http://localhost:9001 | usuario/clave en `.env` |
| Iceberg REST | http://localhost:8181/v1/config | API JSON |
| Spark UI | http://localhost:8080 | sólo visible cuando hay un job corriendo |
| Jupyter Lab | http://localhost:8888 | sin token (imagen `tabulario/spark-iceberg`) |
| MongoDB | mongodb://localhost:27017 | usuario/clave en `.env` |

Si `make smoke` retorna `0`, **Sprint 0 está cerrado**.

### Paso 2 — Descargar datos (Sprints 1 + 1.5)

```bash
# Descargas básicas: MEData, SIMM, GeoMedellín
make download-data

# Rescate de datos reales (Metro xlsx oficial, SIATA Dataverse, EnCicla OSM)
make datos-reales
```

Resultado: ~6 GB en `data/raw/` distribuidos entre las 6 fuentes. Los archivos son ignorados por git (`.gitignore`).

### Paso 3 — Pipeline batch Bronze → Silver → Gold (Sprint 1)

```bash
make init-namespaces       # crea pulsomed.bronze/silver/gold (una sola vez)
make pipeline-batch        # ingesta 6 fuentes + 5 silver + 5 gold
```

O paso por paso si querés debug fino:

```bash
make ingest-bronze-all     # 6 fuentes → Bronze (append + auditoría + HMAC EnCicla)
make transform-silver      # casts, dedup, joins espaciales y temporales
make build-gold            # B-1..B-4 + percentiles_metro (insumo job híbrido)
```

### Paso 4 — Sprint 4 (MapReduce legacy + benchmark formatos)

```bash
make pipeline-legacy       # 3 pasos: generar CSV legacy → mrjob → Bronze MR
make benchmark-formatos    # ADR 04: CSV vs Parquet vs Parquet+ZSTD reproducible
```

### Paso 5 — Sprint 5 (ML + Grafo + Trino)

```bash
make pipeline-sprint5      # train_fatalidad.py + red_metro.py

# Bonus 1 — Trino como tercer motor SQL sobre las mismas tablas Gold
make trino-up              # levanta servicio Trino (puerto 8084)
make trino-demo            # 2 consultas demo (PageRank + accidentalidad)
make trino-sql             # CLI interactivo
```

### Verificación final

```bash
make jupyter               # imprime URL de Jupyter
# Abrir notebooks/01_eda_gold.ipynb, 02_ml_fatalidad.ipynb, 03_eda_completo.ipynb
```

---

## Demo streaming (Sprints 2 + 3)

El camino streaming es independiente del batch. Requiere las referencias precomputadas para el job híbrido (que ya quedan listas tras `make pipeline-batch`, o se exportan desde los CSV reales con `make exportar-referencias`).

### Encendido

```bash
make pipeline-streaming-completo
# Levanta Zookeeper + Kafka + stream-runner; exporta percentiles_metro.json y
# corredores_alta_siniestralidad.json desde los CSV reales.
```

### Productores y jobs (cada uno en su terminal)

| Pregunta | Productor | Job (consumer + ventana + sink) |
|----------|-----------|----------------------------------|
| S-1 EnCicla disponibilidad | `make stream-encicla-producer` | `make stream-encicla-job` |
| S-2 PM2.5 alerta | `make stream-producer` | `make stream-alert-job` |
| S-3 SIMM aforos corredor | `make stream-simm-producer` | `make stream-simm-job` |
| S-4 Metro afluencia RT | `make stream-metro-producer` | `make stream-metro-job` |
| 4.3 Job híbrido (lluvia↔afluencia↔p90 histórico) | (consume de los anteriores) | `make stream-hibrido` |

### Dashboard

```bash
make dashboard             # Streamlit en http://localhost:8501
```

5 paneles + mapa pydeck con 80 estaciones EnCicla reales. Refresca cada 5 segundos consultando MongoDB.

### Consulta CLI rápida

```bash
make stream-alertas ULTIMAS=10min     # últimas alertas PM2.5
```

---

## Análisis y consumo de resultados

Tras correr `make all`, tenés tres formas de consumir las tablas Gold:

### Opción A — Notebooks Jupyter (recomendado para EDA)

```bash
make jupyter
# Abrir http://localhost:8888
```

Notebooks disponibles:

| Notebook | Qué contiene |
|----------|--------------|
| `01_eda_gold.ipynb` | EDA de las 4 tablas Gold del Sprint 1 (B-1..B-4) con gráficas |
| `02_ml_fatalidad.ipynb` | Módulo 06a · pipeline MLlib completo + matriz de confusión + importancia features |
| `03_eda_completo.ipynb` | Bonus 3 · análisis cruzado sobre las 7 tablas Gold |

### Opción B — Trino SQL (bonus Sprint 5)

```bash
make trino-up
make trino-sql
```

```sql
-- Top 5 comunas con más muertos viales
SELECT comuna,
       SUM(con_muertos)  AS total_muertos,
       SUM(con_heridos)  AS total_heridos,
       AVG(indice_severidad) AS severidad_media
FROM iceberg."pulsomed.gold".accidentalidad_por_comuna
GROUP BY comuna
ORDER BY total_muertos DESC
LIMIT 5;

-- Estaciones Metro más centrales (PageRank)
SELECT ranking, nombre, linea, ROUND(pagerank, 6) AS pagerank
FROM iceberg."pulsomed.gold".red_metro_pagerank
ORDER BY ranking
LIMIT 10;
```

### Opción C — PySpark dentro del contenedor

```bash
make pyspark
```

```python
spark.sql("SHOW TABLES IN demo.pulsomed.gold").show()
spark.table("demo.pulsomed.gold.red_metro_rutas_optimas").show()
```

---

## Estructura del repositorio

```
pulso-medellin/
├── docker-compose.yml            # MinIO + Iceberg + Spark + Mongo + Kafka + Trino
├── Makefile                      # Todos los comandos (make help para verlos)
├── .env.example                  # Plantilla; copiar a .env antes de levantar
├── docs/
│   ├── 00-roadmap.md             # Plan de los 6 sprints
│   ├── 01-arquitectura.md        # Stack, namespaces, diagrama
│   ├── instructivo-maestro.md    # Changelog técnico-conceptual por sprint
│   ├── sprints/                  # Una guía operativa por sprint (0..5)
│   └── decisiones/               # ADRs firmados (02, 04, 05, 07)
├── src/
│   ├── shared/                   # config.py (única fuente de verdad) + utils Bronze
│   ├── batch/
│   │   ├── bronze/               # 6 ingest_*.py (uno por fuente)
│   │   ├── silver/transform_all.py
│   │   ├── gold/build_all.py
│   │   ├── ml/train_fatalidad.py     # Módulo 06a Sprint 5
│   │   └── graph/red_metro.py        # Módulo 06b Sprint 5
│   ├── streaming/
│   │   ├── producers/            # 4 producers Kafka (siata, encicla, simm, metro)
│   │   └── flink_jobs/           # 4 jobs ventana + job_hibrido (PyIceberg en vivo)
│   └── legacy/                   # Sprint 4 · MapReduce mrjob (Módulo 01)
├── scripts/
│   ├── download_datasets.sh      # descarga las 6 fuentes base
│   ├── descargar_*_real.py       # Sprint 1.5 · rescate datos reales
│   ├── exportar_referencias_streaming.py
│   ├── benchmark_formatos.py     # ADR 04 reproducible
│   ├── init_iceberg_namespaces.py
│   └── consultar_alertas.py      # CLI Mongo
├── app/
│   └── dashboard.py              # Streamlit (Sprint 3) — http://localhost:8501
├── notebooks/                    # 01_eda_gold + 02_ml_fatalidad + 03_eda_completo
├── tests/smoke/                  # Sprint 0 stack-health (Python + JS)
├── docker/trino/etc/             # Configuración Trino (Sprint 5 bonus)
└── data/
    ├── raw/                      # CSV/XLSX descargados (gitignored)
    └── processed/                # JSONs derivados + modelos (gitignored)
```

---

## Convenciones del equipo

- **Idioma:** código y comentarios en **español**; identificadores técnicos (tablas, tópicos, módulos Python) en **inglés `snake_case`**.
- **Branches:** `main` siempre estable; trabajo en `sprint/N-nombre` o `feat/N-corto`.
- **Commits:** convencionales en español. Ej: `feat(bronze): ingesta MEData con append por lote`.
- **Docs-first:** un módulo no se considera cerrado sin su `.md`.
- **ADRs:** decisiones grandes viven en `docs/decisiones/` (plantilla Nygard).
- **Privacidad (Ley 1581):** `id_usuario` de EnCicla se pseudonimiza con HMAC-SHA256 **antes** de Bronze. La clave vive sólo en `.env`.

---

## Cobertura del curso

| Eje del curso | Sprint(s) | Entregable |
|---------------|-----------|------------|
| Módulo 01 — Hadoop MapReduce | Sprint 4 | `src/legacy/mapreduce_incidentes.py` (mrjob) + `bronze.medata_incidentes_legacy_mr` |
| Módulo 02 — Lambda vs Kappa | Sprint 4 | ADR `docs/decisiones/02-lambda-vs-kappa.md` |
| Módulo 03 — Lakehouse + Iceberg | Sprints 1+2 | 6 ingest Bronze + Silver + Gold sobre Iceberg REST |
| Módulo 04 — Formatos de almacenamiento | Sprint 4 | Benchmark reproducible + ADR 04 |
| Módulo 05 — Delta vs Iceberg | Sprint 4 | ADR 05 (Iceberg con REST Catalog) |
| Módulo 06a — Spark MLlib | Sprint 5 | RandomForest multiclase + métricas en Gold |
| Módulo 06b — GraphX / GraphFrames | Sprint 5 | PageRank + Dijkstra red Metro → Gold |
| Módulo 07 — Cloud + gobernanza Ley 1581/1712 | Sprint 5 | ADR 07 (AWS) + HMAC + controles por capa |
| Streaming Kafka + ventanas | Sprints 2+3 | 4 productores + 4 jobs + job híbrido |
| Bonus 1 — Trino (+2 pt) | Sprint 5 | Servicio Trino sobre las mismas tablas Iceberg |
| Bonus 2 — `make all` end-to-end (+1 pt) | Sprint 5 | `Makefile` target `all` |
| Bonus 3 — Notebook EDA cruzado (+1 pt) | Sprint 5 | `notebooks/03_eda_completo.ipynb` |

---

## Troubleshooting

| Síntoma | Causa probable | Solución |
|---------|----------------|----------|
| `make up` falla con "docker compose: unknown command" | Make no propaga env vars en Windows nativo | Correr desde **WSL**. El equipo usa WSL2. |
| `iceberg-rest` aparece como `unhealthy` | La imagen no trae `curl`/`wget` para healthcheck | Es esperado — no agregar healthcheck. Los clientes hacen retries. |
| `make smoke` falla en `test_iceberg.py` con "namespace not found" | No corriste `make init-namespaces` | `make init-namespaces` antes de cualquier ingesta |
| Bronze SIMM tarda demasiado | El CSV es 816 MB / ~3M filas | Variable `SIMM_LIMIT_FILAS=300000` (por defecto). Para corrida final: `SIMM_LIMIT_FILAS=99999999 make ingest-bronze-simm` |
| `make datos-reales` falla en SIATA | Dataverse API intermitente | Reintentar; los descargados antes no se re-bajan (idempotente) |
| Streaming híbrido emite "PyIceberg falló — fallback a JSON" | El catálogo está caído o falta `make build-gold` | Correr `make pipeline-batch` primero, o aceptar fallback al JSON precomputado |
| Trino devuelve "no such schema" con `demo.pulsomed.gold...` | Trino expone el catálogo como `iceberg`, no `demo` | Usar `iceberg."pulsomed.gold".tabla` (con comillas dobles) |
| Streamlit dashboard no muestra datos | Productores o jobs no corren | Verificar con `docker compose ps` que `stream-runner` esté `Up` y que algún `make stream-*-producer` esté corriendo |
| Errores de memoria en `make pipeline-sprint5` | Spark default heap insuficiente | Aumentar memoria en Docker Desktop a ≥8 GB |

Más diagnóstico operativo por sprint en `docs/sprints/sprint-N-*.md`.

---

## Documentación detallada

| Documento | Cuándo consultarlo |
|-----------|---------------------|
| [`docs/00-roadmap.md`](docs/00-roadmap.md) | Plan global de los 6 sprints y MVP de cada uno |
| [`docs/01-arquitectura.md`](docs/01-arquitectura.md) | Stack, namespaces, diagrama, decisión catálogo |
| [`docs/instructivo-maestro.md`](docs/instructivo-maestro.md) | Changelog técnico-conceptual: qué se entregó cada sprint y por qué |
| [`docs/sprints/sprint-0-setup.md`](docs/sprints/sprint-0-setup.md) | Levantar el stack desde cero |
| [`docs/sprints/sprint-1-batch.md`](docs/sprints/sprint-1-batch.md) | Bronze → Silver → Gold + B-1..B-4 |
| [`docs/sprints/sprint-2-streaming.md`](docs/sprints/sprint-2-streaming.md) | Streaming MVP (S-2 PM2.5) |
| [`docs/sprints/sprint-3-streaming-completo.md`](docs/sprints/sprint-3-streaming-completo.md) | S-1..S-4 + job híbrido + dashboard |
| [`docs/sprints/sprint-4-legacy-y-adrs.md`](docs/sprints/sprint-4-legacy-y-adrs.md) | MapReduce mrjob + ADRs 02/04/05 + refactor datos reales |
| [`docs/sprints/sprint-5-ml-cloud-bonus.md`](docs/sprints/sprint-5-ml-cloud-bonus.md) | MLlib + Grafo + ADR 07 + Trino + EDA cruzado |
| [`docs/decisiones/`](docs/decisiones/) | ADRs firmados (02, 04, 05, 07) |
| [`CLAUDE.md`](CLAUDE.md) | Guía para asistentes IA que trabajen en el repo |

---

## Licencia y créditos

Proyecto académico — uso educativo. Fuentes de datos públicas bajo Ley 1712 (transparencia); pseudonimización HMAC-SHA256 de identificadores conforme a Ley 1581 (protección de datos personales).

Para reportar bugs o preguntas: abrir issue en el repositorio.
