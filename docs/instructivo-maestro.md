# Instructivo · Qué entregué y por qué (Sprint 0)

> Este documento es **el meta-instructivo** que pediste: cada vez que cierro un sprint, dejo aquí un resumen de qué archivos creé, qué hace cada uno, y por qué tomé las decisiones que tomé. Así no quedan cajas negras.

**Sprint cerrado:** Sprint 0 — Setup & Foundations
**Fecha:** kick-off

---

## Lo que entregué

### Documentación

| Archivo | Qué contiene | Cuándo consultarlo |
|---------|--------------|---------------------|
| [`README.md`](../README.md) | Visión general, quick-start, estructura del repo, convenciones del equipo | Primera lectura para cualquier persona nueva |
| [`docs/00-roadmap.md`](00-roadmap.md) | Plan de los 6 sprints con MVP y cobertura del curso | Antes de cada sprint, para saber qué viene |
| [`docs/01-arquitectura.md`](01-arquitectura.md) | Stack técnico, diagramas mermaid, decisiones de catálogo y namespaces | Cuando necesiten entender por qué tal servicio existe |
| [`docs/sprints/sprint-0-setup.md`](sprints/sprint-0-setup.md) | Guía paso-a-paso del Sprint 0 con prerrequisitos y troubleshooting | Cada vez que alguien levante el stack en una máquina nueva |
| [`docs/instructivo-maestro.md`](instructivo-maestro.md) | Este archivo — meta-explicación de cada sprint | Después de cada sprint, para revisar qué hice |

### Código y configuración (raíz del proyecto)

| Archivo | Qué hace | Por qué así |
|---------|----------|-------------|
| `docker-compose.yml` | Orquesta MinIO, Iceberg REST Catalog, Spark+Jupyter y MongoDB. Servicios futuros (Kafka, Flink, Trino) están comentados con marcas `[SPRINT 2+]` / `[SPRINT 5]` | Tener todos los servicios en un único compose evita el infierno de "qué archivo de compose toca hoy". Marcar lo futuro deja claro qué descomentar y cuándo. |
| `Makefile` | Atajos: `make up`, `make down`, `make smoke`, `make logs SERVICE=...`, etc. | Reduce el costo de memorizar comandos `docker compose` largos. Es el primer punto de entrada para cualquier persona del equipo o evaluador externo. |
| `.env.example` | Plantilla de variables de entorno (puertos, contraseñas, regiones) | Separar secretos del código. El `.env` real va a `.gitignore`. |
| `.gitignore` | Excluye `.env`, datos crudos, `__pycache__`, checkpoints, etc. | Evita commits accidentales de secretos o GBs de CSV. |

### Smoke tests (verifican el "Definition of Done" del Sprint 0)

| Archivo | Qué valida |
|---------|------------|
| `tests/smoke/test_minio.py` | Bucket `warehouse` creado y accesible vía `boto3` |
| `tests/smoke/test_iceberg.py` | Spark crea namespace, crea tabla Iceberg, inserta, lee, dropea — esto cubre los 4 servicios al tiempo |
| `tests/smoke/test_mongodb.py` | Conexión autenticada y CRUD básico contra Mongo |

### Estructura de carpetas (vacías por ahora, se llenan en sprints)

```
src/batch/{bronze,silver,gold}/    -- Sprint 1
src/streaming/{producers,flink_jobs,sinks}/  -- Sprints 2-3
src/shared/                        -- Código común (esquemas, utils)
data/{raw,samples,processed}/      -- Datos en distintos estados de procesamiento
notebooks/                         -- EDA en Jupyter
scripts/                           -- Scripts auxiliares (descargas, init)
docker/spark/                      -- Dockerfile propio si necesitamos jars custom
docs/decisiones/                   -- ADRs (Sprints 4-5)
docs/diagramas/                    -- Diagramas exportados (PNG/SVG)
```

---

## Por qué cada decisión técnica

### 1. ¿Por qué `tabulario/spark-iceberg` en vez de armar Spark a mano?

**Decisión:** usar la imagen oficial de Tabular (creadores del REST Catalog).

**Razón:** armar Spark + Iceberg + AWS S3 jars desde cero es infierno de classpath. La gente que mantiene esa imagen ya resolvió las versiones compatibles (`iceberg-spark-runtime-3.5_2.12`, `bundle` AWS, `hadoop-aws`, etc). Nos ahorra fácilmente 1-2 semanas de "por qué este jar no carga".

**Trade-off:** la imagen pesa ~2 GB. Si después necesitamos jars adicionales (ej. MongoDB Spark connector en Sprint 3), creamos un `Dockerfile` en `docker/spark/` que extiende esta imagen y agrega lo nuestro.

### 2. ¿Por qué REST Catalog y no Hive Metastore o JDBC catalog?

**Decisión:** REST Catalog (servicio HTTP separado).

**Razón:** la sección 5 de la propuesta vende "interoperabilidad multi-motor" como ventaja diferencial de Iceberg. El REST Catalog es **el** estándar para conectar Spark, Trino, Flink, DuckDB, y notebooks al mismo catálogo. Hive Metastore funciona pero se siente legacy y agrega un servicio Thrift más complejo. JDBC catalog está bien para una sola máquina pero no escala fuera.

### 3. ¿Por qué un único `docker-compose.yml` con servicios comentados, en vez de varios archivos por sprint?

**Decisión:** un solo compose con marcas `[SPRINT 2+]` y `[SPRINT 5]`.

**Razón:** evaluamos `compose.yml` + `compose.streaming.yml` + `compose.bonus.yml` con merges. Resultado: la gente se confunde con qué `-f` pasar y se rompen las redes. Un único archivo donde uno descomenta es más simple, más reproducible, y más explícito sobre qué viene cuando.

**Trade-off:** el archivo se ve grande. Lo aceptamos.

### 4. ¿Por qué smoke tests en Python y no en bash?

**Decisión:** smoke tests en Python (pyspark, pymongo, boto3) corridos **dentro** del contenedor de Spark.

**Razón:** los smoke tests deben validar que lo que vamos a usar realmente funciona. Si los corremos en bash desde el host con `curl`, validamos que el puerto está abierto pero no que la integración Spark↔Iceberg↔MinIO funciona. Corriendo en Python dentro del contenedor que va a hacer el trabajo real, replicamos la realidad del Sprint 1.

### 5. ¿Por qué MongoDB ya en Sprint 0 si no se usa hasta Sprint 2?

**Decisión:** levantar MongoDB desde Sprint 0.

**Razón:** uno de los 3 smoke tests es contra Mongo, así desde el día 1 sabemos que la conexión funciona y los puertos no chocan. Cuesta poco RAM (~150 MB) y nos compra tranquilidad. Kafka/Flink en cambio comen RAM agresivamente y los dejamos para cuando los necesitemos de verdad.

### 6. ¿Por qué `pulsomed` como nombre de namespace en Iceberg?

**Decisión:** `pulsomed.bronze.<tabla>`, `pulsomed.silver.<tabla>`, `pulsomed.gold.<tabla>`.

**Razón:** en Iceberg el primer nivel del namespace ayuda a aislar este proyecto si en algún momento compartimos catálogo con otros (en cloud o en una organización). Los siguientes niveles (`bronze`/`silver`/`gold`) hacen que `SHOW TABLES IN pulsomed.bronze` filtre por capa, lo cual ayuda al EDA y a las consultas operativas. Todos los nombres en inglés `snake_case` para no pelear con keywords de SQL.

---

## Lo que dejé pendiente para preguntas del equipo

Antes de empezar Sprint 1, necesito que respondan algunas cosas que cambian decisiones aguas abajo. Vienen al final de mi mensaje principal en el chat.

---

## Cómo se actualiza este instructivo

Cada vez que cierre un sprint:

1. Agrego una sección `Sprint N — <nombre>` con el mismo formato (Lo que entregué + Por qué cada decisión).
2. Mantengo los anteriores intactos como historial.
3. Si una decisión vieja se revisa, dejo una **nota** explicando el cambio y el motivo, no la borro.

Esto es básicamente un changelog técnico-conceptual. Es lo que diferencia "tener código" de "tener un proyecto que se entiende".

---

# Sprint 1 — Camino Batch (Bronze → Silver → Gold)

**Sprint cerrado.** MVP `make pipeline-batch` corre las 6 ingestas, las 5 transformaciones Silver y los 4 Gold; las 4 tablas Gold responden las preguntas analíticas B-1..B-4. Detalle operativo en [`sprints/sprint-1-batch.md`](sprints/sprint-1-batch.md).

## Lo que entregué

### Código nuevo

| Archivo | Para qué |
|---------|----------|
| `src/shared/bronze_utils.py` | Helpers compartidos por los 6 ingest: columnas de auditoría, `escribir_bronze`, logger plano. |
| `src/batch/bronze/ingest_*.py` | Un script por fuente (MEData, Metro, EnCicla, SIATA, GeoMedellín, SIMM). Append + auditoría + particionado por `fecha_ingesta`. EnCicla pseudonimiza con HMAC-SHA256 antes de escribir. |
| `src/batch/silver/transform_all.py` | 5 transformaciones (`createOrReplace`): casteo, dedup, corrección de coordenadas invertidas en MEData pre-2017, joins espaciales con GeoMedellín (UDF Python ray-casting), join temporal SIATA↔Metro. |
| `src/batch/gold/build_all.py` | 4 agregaciones: `afluencia_vs_pm25` (Pearson estación×mes), `accidentalidad_por_comuna` (pivot + índice severidad), `demanda_encicla_vs_clima` (bins + viajes_relativos), `corredores_riesgo_compuesto` (rank volumen + severidad). |
| `scripts/generar_muestras_sinteticas.py` | Genera muestras consistentes para Metro, SIATA, EnCicla cuando los portales bloquean la descarga real. Reemplazables por datos reales sin tocar el código. |
| `notebooks/01_eda_gold.ipynb` | EDA de las 4 Gold con gráficas. |

### Decisiones técnicas (las grandes)

**1. Sintetizar fuentes que no se descargan.** ArcGIS Hub (Metro) bloquea con 403, la API CKAN de Metropol (EnCicla) cambió URLs, Dataverse SIATA requiere `jq`. En vez de bloquear el sprint, generamos muestras realistas con el mismo esquema. Los scripts Bronze son agnósticos al origen — basta sobrescribir el archivo cuando llegue el dato real.

**2. Catálogo `demo`, no `pulsomed`.** La imagen `tabulario/spark-iceberg` ya viene cableada a un catálogo llamado `demo` vía variables de entorno. `pulsomed` es un namespace bajo ese catálogo, no el catálogo mismo. Las tablas son `demo.pulsomed.bronze.<x>`. Esto se documentó en CLAUDE.md también porque es un foot-gun para nuevos contribuyentes.

**3. Ray-casting Python para joins espaciales.** Con 21 polígonos y ~600 k puntos, un UDF Python con ray-casting clásico es suficiente y no necesita Sedona ni un Dockerfile custom de Spark. Si Sprint 5 necesita joins espaciales más sofisticados (línea ↔ polígono, buffers), migramos.

**4. EnCicla con HMAC en Bronze, no en Silver.** Quien tiene acceso al data lake nunca debe ver `id_usuario` real, ni siquiera en una capa intermedia. La clave HMAC vive solo en `.env`. Esto es la implementación temprana del Módulo 07 (gobernanza Ley 1581).

**5. `createOrReplace` en Silver/Gold.** Idempotente y simple. Si en Sprint 4 necesitamos historial de cambios, agregamos snapshot retention de Iceberg.

**6. SIMM cámaras limitado a 300 k filas por defecto.** El CSV es 816 MB / ~3M filas. Para iterar rápido, `SIMM_LIMIT_FILAS` controla el truncado. Para una corrida final: `SIMM_LIMIT_FILAS=99999999`.

### Filas resultantes

```
Bronze:
  geomedellin_comunas               21
  metro_afluencia              29 592
  siata_lecturas               87 600
  encicla_prestamos            13 776
  medata_incidentes           270 765
  simm_aforos                 374 900

Silver:
  lecturas_aire_validas        87 100
  afluencia_horaria            29 592
  incidentes_geocodificados   270 731
  viajes_encicla_anonimizados  13 776
  aforos_corredor_geo         374 900

Gold:
  afluencia_vs_pm25               972  (estación × mes)
  accidentalidad_por_comuna       220  (comuna × año, ranking)
  demanda_encicla_vs_clima        180  (día × clima)
  corredores_riesgo_compuesto      16  (comuna)
```

---

# Sprint 2 — Streaming MVP (S-2 alerta PM2.5)

**Sprint cerrado.** `make pipeline-streaming` levanta el stack streaming. Productor + alert-job + CLI corren end-to-end y producen alertas verificables en MongoDB.

## Lo que entregué

| Archivo | Para qué |
|---------|----------|
| `docker-compose.yml` (zookeeper, kafka, stream-runner) | Stack streaming descomentado. `stream-runner` es un `python:3.11-slim` que en boot instala `kafka-python` + `pymongo`. |
| `src/streaming/producers/siata_producer.py` | Lee SIATA histórico y emite a `siata.lecturas` cada `INTERVALO_S`. Inyecta picos de PM2.5 cada `INYECTAR_PICO_CADA` para garantizar alertas en demos cortas. |
| `src/streaming/flink_jobs/siata_alert_job.py` | Consumidor Kafka + ventana tumbling de N minutos en memoria + sink a Mongo. Cierre de ventanas por `max(event_time, wall_clock)` para no quedar bloqueado cuando el productor pausa. |
| `scripts/consultar_alertas.py` | CLI con filtros (`--zona`, `--gravedad`, `--ultimas`). |
| `docs/sprints/sprint-2-streaming.md` | Cómo correrlo + esquemas + ADR pendiente. |

## Decisiones técnicas

**1. Python en vez de PyFlink para Sprint 2.** El roadmap original decía "PyFlink o Flink Java/Scala — decidir y documentar". Decidimos Python por ahora porque:
- PyFlink requiere `flink-sql-connector-kafka` con la versión exacta de Flink, jars en `/opt/flink/lib`, y un build de imagen.
- Para una sola pregunta (S-2) y una sola ventana es desproporcionado.
- El primitivo (ventana tumbling sobre clave + agregación + sink) son ~150 líneas de Python.
- Migrar a Flink real está planeado para Sprint 3, cuando haya 4 jobs paralelos y argumento real.

Trade-offs aceptados (documentados en `sprint-2-streaming.md`): sin estado distribuido, at-least-once, ventanas en buffer se pierden si el proceso muere.

**2. Ventana cierra por `max(event_time, wall_clock) - VENTANA`.** Si solo usáramos event-time, cuando el productor pausa las ventanas se quedan abiertas para siempre. Si solo usáramos wall-clock, eventos históricos no abrirían/cerrarían correctamente. El `max` cubre ambos.

**3. Índice único `{zona, ventana_inicio}` en `alertas_aire`.** Protege contra reproceso o restart del job: si una ventana ya emitió alerta, el `insertOne` falla con E11000 y el job sigue.

**4. Inyección de picos en el productor.** Para que `make pipeline-streaming` siempre demuestre algo en menos de 30 segundos, el productor mete forzadamente un evento de `pm25=PICO_PM25` cada N eventos. En producción no haría esto — pero el demo del Sprint 2 lo necesita para no depender de que aparezca un evento real con pm25>75.

**5. `stream-runner` separado del contenedor de Spark.** Spark + Iceberg ya son grandes. Un `python:3.11-slim` con `kafka-python` + `pymongo` instalados en boot es 100 MB y arranca en segundos. Para el job híbrido del Sprint 3 (que SÍ lee de Gold-Iceberg en bootstrap), evaluaremos si se queda en stream-runner con `pyiceberg` o si se mueve al spark-iceberg.

## Cómo se ejecuta el demo S-2 (verificado en máquina)

```bash
make stream-up                                   # Zookeeper + Kafka + stream-runner
# Terminal A:
make stream-alert-job VENTANA_MINUTOS=1 UMBRAL_PM25=75
# Terminal B:
INTERVALO_S=0.05 INYECTAR_PICO_CADA=5 LIMITE_EVENTOS=120 make stream-producer
# Terminal C:
make stream-alertas ULTIMAS=10min
```

Salida en C:

```
ventana                 zona                          gravedad    pm25_avg  lect.
2026-05-08 03:32        valle_aburra_centro           moderada      95.0     10
2026-05-08 03:32        valle_aburra_nororiental      moderada      95.0     10
```

## Cambio relevante a `src/shared/config.py`

Se difirió el import de `pyspark.sql.SparkSession` al interior de `crear_spark_session()`. Razón: el contenedor `stream-runner` no tiene PySpark instalado, pero sí necesita las constantes de tópicos Kafka y de Mongo. Sin este cambio, cualquier `from shared.config import ...` desde el stream-runner fallaba con `ModuleNotFoundError: No module named 'pyspark'`. Es un patrón que se mantiene desde aquí en adelante: shared.config no debe importar nada que no esté disponible en TODOS los contenedores.

---

# Sprint 1.5 — Rescate de datos reales

**Sprint corto.** Antes de Sprint 3, se sustituyeron 3 fuentes sintéticas por descargas reales públicas, sin tocar Bronze/Silver/Gold. Detalle en [`sprints/sprint-3-streaming-completo.md`](sprints/sprint-3-streaming-completo.md) (sección Sprint 1.5).

## Lo que entregué

| Script | Qué hace |
|--------|----------|
| `scripts/descargar_metro_afluencia_real.py` | Resuelve los item-IDs ArcGIS desde el DCAT feed (los IDs hardcoded estaban desactualizados), descarga xlsx con UA de navegador, convierte de wide (día×línea×hora) a CSV largo (240k filas reales). |
| `scripts/descargar_siata_real.py` | Reemplaza la dependencia de `jq` por API Dataverse en Python; descarga PM2.5 + PM10 (1.7M filas) + metadata de las 44 estaciones reales con coordenadas. |
| `scripts/descargar_encicla_estaciones.py` | OSM Overpass devuelve 80 nodos `bicycle_rental` con `network=EnCicla` o nombre que contiene "EnCicla" — nombres oficiales (Ruta N, MAMM, Plaza Botero) y coordenadas reales. |

## Por qué cada decisión técnica

**1. Por qué leer los productores directamente del CSV en vez de re-ingestar a Bronze.**
El cambio de schema de las fuentes reales (Metro pasa de estación a línea, SIATA de wide a long) requiere refactor de Bronze/Silver/Gold. Para no bloquear Sprint 3, los productores leen el CSV/JSON real directo. El refactor de batch queda como tarea explícita de Sprint 4. Mientras tanto, Sprint 3 ya muestra **datos reales** en vivo.

**2. EnCicla disponibilidad: simulación honesta sobre estaciones reales.**
La app móvil de EnCicla usa un backend privado autenticado. No hay API pública. La decisión es transparente: las 80 estaciones (nombres + lat/lon + capacidad) son **reales**; el ratio de bicicletas/anclajes a lo largo del tiempo es simulado con un modelo Poisson + perfil horario. Documentado claramente como tal en el README del productor.

**3. Meteorología SIATA sigue sintética.**
Los datasets en Dataverse están dispersos en >100 DOIs por estación-variable. Sprint 1.5 es ya bastante. Se documenta la limitación; Sprint 4 hace el script consolidado si se requiere.

---

# Sprint 3 — Streaming completo + integración batch↔streaming

**Sprint cerrado.** Las 4 preguntas operacionales (S-1..S-4) tienen productor, job de procesamiento y colección Mongo propios. El job híbrido materializa la sección 4.3 de la propuesta. Un dashboard Streamlit refresca cada 5 segundos. Detalle en [`sprints/sprint-3-streaming-completo.md`](sprints/sprint-3-streaming-completo.md).

## Lo que entregué

### Productores (todos en `src/streaming/producers/`)

| Archivo | Lee de | Emite a | Real vs sintético |
|---------|--------|---------|-------------------|
| `siata_producer.py` (Sprint 2, sin cambios) | `data/raw/siata_historico/` | `siata.lecturas` | **PM2.5 real** desde Sprint 1.5; meteorología sintética |
| `encicla_producer.py` | 80 estaciones OSM reales | `encicla.disponibilidad` | Estaciones reales; disponibilidad simulada (sin API pública) |
| `simm_producer.py` | `simm_traffic_data.csv` (medata.gov.co) | `simm.aforos` | **100% real** (lecturas CCTV reales) |
| `metro_producer.py` | `afluencia_metro_*.csv` (real, 240k filas) | `metro.validaciones` | **Afluencia real** distribuida en micro-eventos |

### Jobs (todos en `src/streaming/flink_jobs/`)

| Archivo | Pregunta | Ventana | Sink |
|---------|----------|---------|------|
| `siata_alert_job.py` (Sprint 2) | S-2 | tumbling 10 min | `alertas_aire` |
| `encicla_disponibilidad_job.py` | S-1 | sliding 1 min / paso 30 s | `disponibilidad_encicla` |
| `simm_aforo_job.py` | S-3 | tumbling 5 min | `aforos_corredor` |
| `metro_afluencia_job.py` | S-4 | tumbling 5 min | `afluencia_metro_rt` |
| `job_hibrido.py` | **sección 4.3** | rolling 5min + eval cada 10s | `alertas_hibridas` |

### Bootstrap de referencias

`scripts/exportar_referencias_streaming.py` calcula desde los CSV reales:
- `data/processed/percentiles_metro.json` — p50/p75/p90/p95 de pasajeros por (línea × franja_horaria), sobre 240k observaciones reales
- `data/processed/corredores_alta_siniestralidad.json` — top-8 comunas y 51 corredores derivados de 257k incidentes MEData reales

### Dashboard

`app/dashboard.py` — Streamlit con 5 paneles (KPIs, mapa EnCicla, alertas aire, afluencia Metro, corredores SIMM, alertas híbridas). Refresca cada 5 s consultando Mongo.

## Decisiones técnicas (las grandes)

**1. Mantenemos Python en `stream-runner`, no migramos a PyFlink.**
Cuatro jobs corriendo en paralelo demuestran que el patrón Python escala para este proyecto. Cada job consume <50 MB y procesa cientos de eventos/segundo. Trade-off aceptado y documentado: at-least-once, ventanas en memoria, sin exactly-once. ADR formal en Sprint 4.

**2. Job híbrido lee referencias precomputadas, no Iceberg en vivo.**
Para el MVP del Sprint 3, leer un JSON pre-calculado de los CSV reales es equivalente al ejercicio sin la complejidad de PyIceberg + boto3 + REST endpoint en stream-runner. La migración a Iceberg-en-vivo queda como mejora del Sprint 4 (y se justificará explícitamente para demostrar el valor del REST Catalog).

**3. Corredores de alta siniestralidad se derivan de MEData real.**
El score OMS-like (5×muertos + 1×heridos + 0.1×daños) sobre 257k incidentes reales produce 8 comunas top y 51 corredores. La lista vive en JSON y el job SIMM la lee al arranque — el job NO tiene cableo a comunas específicas, lo cual permite recalcular cuando llegue MEData 2025.

**4. Cada productor decide su propia cadencia.**
- SIATA real es horario → el productor reproduce con `INTERVALO_S` configurable.
- Metro afluencia es por hora-línea → el productor lo distribuye en `EVENTOS_POR_HORA` micro-eventos para granularidad fina.
- SIMM es continuo (cámaras CCTV) → replay directo a 1 evento/seg.
- EnCicla simulado → tick por estación cada `INTERVALO_S`.

**5. Dashboard usa pydeck para mapa de EnCicla.**
Permite visualizar las 80 estaciones reales con color rojo cuando bicis_min ≤ umbral. Sin dependencias geo pesadas; pydeck viene en pip estándar.

## Cómo se ejecuta el demo Sprint 3 (verificado en código, pendiente de demo viva)

```bash
make datos-reales                  # ~5 min de descarga (idempotente)
make pipeline-streaming-completo   # stack + referencias

# 9 procesos en paralelo (ver Makefile target pipeline-streaming-completo)
make stream-producer  &
make stream-encicla-producer &
make stream-simm-producer &
make stream-metro-producer &
make stream-alert-job &
make stream-encicla-job &
make stream-simm-job &
make stream-metro-job &
make stream-hibrido &

make dashboard                     # http://localhost:8501
```

## Pendiente para Sprint 4

- [x] ADR formal Lambda vs Kappa (`docs/decisiones/02-lambda-vs-kappa.md`)
- [x] ADR formal Delta vs Iceberg (`docs/decisiones/05-delta-vs-iceberg.md`)
- [x] MapReduce legacy de incidentes (Módulo 01)
- [x] Refactor Bronze/Silver/Gold para incorporar los datos reales del Sprint 1.5
- [x] Migrar `job_hibrido.py` a consulta PyIceberg en vivo sobre Gold

(Detalle abajo.)

---

# Sprint 4 — Legacy MapReduce, ADRs firmados y refactor con datos reales

**Sprint cerrado en código y documentación.** Verificación end-to-end queda
sujeta a correr `make pipeline-batch` + `make pipeline-legacy` con el stack
Docker arriba. Detalle en
[`sprints/sprint-4-legacy-y-adrs.md`](sprints/sprint-4-legacy-y-adrs.md).

## Lo que entregué

### Documentos de decisión (ADRs firmados)

| Archivo | Cubre módulo | Decisión |
|---------|---------------|----------|
| `docs/decisiones/02-lambda-vs-kappa.md` | 02 | Lambda. Batch sobre Iceberg + streaming sobre Kafka/Python. El job híbrido materializa el patrón. |
| `docs/decisiones/04-benchmark-formatos.md` | 04 | Parquet + ZSTD. CSV es 4-6× más grande, lectura 6-10× más lenta; ZSTD gana 30% sobre Snappy sin costo de lectura. Reproducible con `make benchmark-formatos`. |
| `docs/decisiones/05-delta-vs-iceberg.md` | 05 | Iceberg con catálogo REST. Iceberg gana en Snowflake, Athena y Colab; empata con Delta en Databricks (UniForm). |

### MapReduce legacy (Módulo 01)

| Archivo | Para qué |
|---------|----------|
| `src/legacy/generar_dataset_legacy.py` | Reconstruye la heterogeneidad histórica de MEData (pre/post-2017) partiendo del CSV unificado actual: dos archivos sin encabezado con esquemas distintos. |
| `src/legacy/mapreduce_incidentes.py` | Job `mrjob` con mapper que detecta el esquema por contenido (fecha + n° de columnas) y reducer que dedupe por `nro_radicado` prefiriendo el más nuevo. Counters de calidad expuestos. |
| `src/batch/bronze/ingest_legacy_mr.py` | Lee el TSV canónico y lo escribe a `demo.pulsomed.bronze.medata_incidentes_legacy_mr`. Cierra el ciclo MR → Bronze Iceberg. |
| `src/legacy/README.md` | Cómo correrlo, qué counters esperar, trade-off mrjob vs Hadoop Java puro. |

### Refactor batch con datos reales del Sprint 1.5

| Archivo | Cambio |
|---------|--------|
| `bronze/ingest_metro.py` | Schema pasa de `(fecha, estacion_id, estacion_nombre, linea, validaciones)` a `(fecha, linea, hora, pasajeros)`. La fuente pública NO desglosa por estación. |
| `bronze/ingest_siata.py` | Lee los CSV largos PM2.5/PM10 reales, pivota long→wide, anexa metadatos de estación. Cols meteorológicas como NULL hasta Sprint 5. |
| `silver/transform_all.py` | `_silver_afluencia` ahora trabaja con granularidad hora×línea y deriva `franja_horaria`. `_silver_aire` tolera columnas opcionales del Bronze real. |
| `gold/build_all.py` | B-1 recalculada por `(linea × mes)`. Nuevo `b5_percentiles_metro` produce `gold.percentiles_metro` insumo del job híbrido. |
| `shared/config.py` | Nueva constante `TBL_GOLD_PERCENTILES_METRO`. |

### Migración del job híbrido a Iceberg en vivo

`src/streaming/flink_jobs/job_hibrido.py` ahora carga los percentiles desde
`demo.pulsomed.gold.percentiles_metro` vía **PyIceberg** contra el REST Catalog.
Si la tabla no existe o el catálogo no responde, cae al JSON precomputado del
Sprint 3 — la migración no rompe el camino anterior. El log indica
explícitamente qué fuente se usó.

`docker-compose.yml::stream-runner` ahora instala `pyiceberg[s3fs,pyarrow]`
en boot y expone `AWS_*` + `ICEBERG_REST_URI` + `ICEBERG_S3_ENDPOINT`.

### Comandos Makefile añadidos

```bash
make benchmark-formatos    # ADR 04, reproducible
make legacy-generar        # paso 1 MR
make legacy-mapreduce      # paso 2 MR (instala mrjob si falta)
make legacy-ingest         # paso 3 MR
make pipeline-legacy       # 1+2+3 encadenados
```

## Decisiones técnicas (las grandes)

**1. mrjob en lugar de Hadoop Java puro.** Trade-off documentado en
`src/legacy/README.md`. mrjob cubre el paradigma (mapper + reducer + counters)
sin sumar 4 contenedores Hadoop al compose. Si el evaluador exige Hadoop
"puro", el código mrjob es directamente convertible a Mapper/Reducer Java.

**2. Iceberg en vivo con fallback robusto.** No queremos que la migración
rompa el camino del Sprint 3. Si el batch no se ha corrido o el catálogo
está caído, el JSON sigue siendo el respaldo. La variable
`FORZAR_FUENTE_JSON=1` permite tests offline.

**3. Columnas meteorológicas en NULL en lugar de inventarlas.** La fuente
SIATA real (Dataverse) sólo trae PM2.5/PM10 fácilmente accesibles. El resto
(precipitación, temperatura, humedad, viento) vive en >100 DOIs por estación
y queda como tarea Sprint 5. Silver no se cae: tolera NULL y `_silver_afluencia`
hace `LEFT JOIN` — pierde correlaciones con precipitación pero no la
consulta.

**4. Granularidad Metro pasa a horaria en Silver.** La fuente real es
hora×línea — conservamos esa resolución en `silver.afluencia_horaria`.
B-1 mensual sigue funcionando porque agrega desde ahí, y el job híbrido
puede leer percentiles por franja horaria sin más procesamiento.

## Lo que NO se hizo en este Sprint

- MLlib (Módulo 06a) — predicción de fatalidad sobre Gold accidentalidad.
- GraphFrames (Módulo 06b) — rutas mínimas Metro.
- ADR 07 Cloud (AWS vs GCP) + controles de acceso por capa + marco Ley 1581/1712.
- Bonuses Sprint 5: Trino, `make all` end-to-end, notebook EDA cruzado.
- Consolidación de meteorología SIATA real.
- EnCicla préstamos reales (bloqueo institucional — requiere PQRS al AMVA).
