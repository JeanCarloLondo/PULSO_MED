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
