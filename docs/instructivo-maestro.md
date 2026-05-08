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
