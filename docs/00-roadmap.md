# Roadmap de Sprints · Pulso Medellín

Plan de trabajo dividido en **6 sprints**. Cada sprint tiene un **MVP** verificable y al menos **un eje del curso** cubierto. El orden está diseñado para que cada sprint construya sobre el anterior y para que en cualquier punto el proyecto sea presentable.

---

## Sprint 0 — Setup & Foundations

**Objetivo:** que cualquiera del equipo pueda clonar el repo y levantar el stack completo con un solo comando.

**Entregables:**
- Estructura del repositorio.
- `docker-compose.yml` con: MinIO, Iceberg REST Catalog, Spark (master + worker con jars de Iceberg), MongoDB.
- `Makefile` con `up`, `down`, `ps`, `logs`, `smoke`, `clean`.
- Smoke tests que verifican: MinIO responde, REST Catalog levanta, Spark crea una tabla Iceberg de prueba, MongoDB acepta inserts.
- `docs/sprints/sprint-0-setup.md` con la guía paso a paso.

**MVP:** `make up && make smoke` retorna `0` en una máquina limpia con Docker.

**Lo que NO se hace en este sprint:** Kafka, Flink, ni ingesta de datos reales. Eso entra en Sprints 1 y 2.

---

## Sprint 1 — Camino Batch end-to-end (Bronze → Silver → Gold)

**Objetivo:** responder las preguntas analíticas B-1 a B-4 con datos reales de las fuentes batch (MEData, GeoMedellín, Metro CSV histórico, EnCicla CSV, SIATA Kaggle, SIMM CSV).

**Entregables:**
- Scripts PySpark de **ingesta a Bronze** con append por lote, particionado por `fecha_ingesta` y metadatos de auditoría (`timestamp_ingesta`, `nombre_archivo`, `fuente_id`).
- Scripts PySpark de **transformación a Silver**: deduplicación, casting, corrección de coordenadas invertidas (MEData), reproyección CRS, joins espaciales con GeoMedellín, joins temporales SIATA↔Metro.
- Scripts PySpark de **agregación a Gold**: 4 tablas que responden las preguntas B-1, B-2, B-3, B-4.
- **Benchmark de formatos** (Módulo 04): comparación CSV vs Parquet vs Parquet+ZSTD con métricas de tamaño, tiempo de escritura, tiempo de lectura, partition pruning. Reporte en `docs/decisiones/04-benchmark-formatos.md`.
- Tests de calidad de datos (great_expectations o asserts manuales) por capa.
- Notebook `notebooks/01_eda_gold.ipynb` que consulta Gold y muestra las respuestas a B-1..B-4 con gráficas.

**MVP:** `make pipeline-batch` corre el Bronze→Silver→Gold completo con datos reales y al final un notebook muestra cuatro gráficas que responden las cuatro preguntas batch.

**Decisiones técnicas a documentar:**
- Por qué Iceberg (vs Parquet plano) — semilla del Módulo 05.
- Estrategia de particionamiento por capa.
- Tratamiento del cambio de esquema de MEData en 2017 (el `barrio` → `barrio_accidente`).

---

## Sprint 2 — Camino Streaming MVP (Pregunta S-2)

**Objetivo:** demostrar el camino streaming end-to-end con la pregunta más simple: alerta de PM2.5.

**Entregables:**
- Extensión de `docker-compose.yml`: añadir Zookeeper, Kafka, Flink JobManager, Flink TaskManager.
- Productor Kafka en Python que simula lecturas SIATA cada 10 minutos (modo dev: cada 10 segundos para acelerar pruebas) leyendo del histórico Kaggle como fuente.
- Job Flink (PyFlink o Java/Scala — decidir y documentar) con ventana **tumbling de 10 minutos** que agrega PM2.5 por zona y emite alerta cuando supera 75 µg/m³.
- Sink a MongoDB en colección `alertas_aire` con índice compuesto por `(zona, timestamp)`.
- Script de consulta: `scripts/consultar_alertas.py --zona poblado --ultimas 1h`.
- Documentación del esquema JSON del tópico `siata.lecturas` y de la colección `alertas_aire`.

**MVP:** correr el productor 5 minutos y ver al menos una alerta insertada en MongoDB consultable por CLI.

**Decisiones técnicas a documentar:**
- PyFlink vs Flink Java/Scala (impacta la curva de aprendizaje y el rendimiento).
- Semántica de entrega: `at-least-once` para empezar, evaluar `exactly-once` después.
- Estrategia de retención de tópicos (mínimo 7 días para replay).

---

## Sprint 3 — Streaming completo + integración batch↔streaming

**Objetivo:** las cuatro preguntas operacionales (S-1 a S-4) corriendo y la integración real con Gold.

**Entregables:**
- Productor Kafka EnCicla (disponibilidad cada 5 min) + job Flink ventana sliding 1 min/30s → colección `disponibilidad_encicla`.
- Productor Kafka Metro (validaciones torniquete cada 30s) + job Flink ventana tumbling 5 min → colección `afluencia_metro_rt`.
- Productor Kafka SIMM (aforos cada 60s) + job Flink → colección `aforos_corredor`.
- **Job de integración:** lee el percentil 90 histórico desde Gold (Iceberg) al arranque, lo cachea, y emite alerta híbrida cuando la afluencia RT cae por debajo del umbral histórico para esa franja con ese nivel de precipitación. Esto materializa la sección 4.3 de la propuesta.
- Dashboard simple (Streamlit o FastAPI + frontend mínimo) que consulta MongoDB cada 5 segundos.

**MVP:** las cuatro preguntas S-1, S-2, S-3, S-4 son consultables vía CLI o dashboard, y al menos una alerta híbrida se dispara cuando se inyecta una lluvia simulada.

**Decisiones técnicas a documentar:**
- Diseño del modelo documental MongoDB por colección.
- Estrategia de bootstrap del job híbrido (cómo lee Gold sin convertir a Spark Streaming).

---

## Sprint 4 — Legado y documentos de decisión

**Objetivo:** cubrir los ejes del curso que no son end-to-end y dejar firmados los ADRs grandes.

**Entregables:**
- **Módulo 01 — Arqueología de datos:** job Hadoop MapReduce (Java o Python con `mrjob`) que procesa los CSV de incidentes viales con dos esquemas distintos (pre/post 2017) y formato sin encabezado. Salida normalizada que se ingesta a Bronze.
- **ADR Módulo 02 — Lambda vs Kappa:** documento en `docs/decisiones/02-lambda-vs-kappa.md` con análisis de los 3 SLAs del proyecto, alternativas evaluadas, decisión justificada.
- **ADR Módulo 05 — Delta vs Iceberg:** documento en `docs/decisiones/05-delta-vs-iceberg.md` con benchmark de los 4 consumidores (Databricks, Snowflake, Athena, Colab) y la decisión final.
- Refinamiento del benchmark de formatos del Sprint 1.

**MVP:** los tres documentos están firmados (status: aceptado) y el job MapReduce produce un CSV normalizado que se valida contra la salida de Bronze.

---

## Sprint 5 — ML, Cloud y Bonus

**Objetivo:** cerrar los módulos restantes y los bonus del curso.

**Entregables:**
- **Módulo 06a — MLlib:** modelo de predicción de fatalidad en incidentes viales sobre Gold. Pipeline ML completo (split, feature engineering con UDFs, entrenamiento, evaluación). Notebook reproducible.
- **Módulo 06b — GraphX (o GraphFrames si trabajamos en PySpark):** cálculo de rutas de menor tiempo en la red Metro. Salida: tabla Gold con shortest path entre cada par de estaciones.
- **Módulo 07 — Cloud y gobernanza:**
  - ADR `docs/decisiones/07-cloud-aws-vs-gcp.md` con la decisión final.
  - Implementación de la pseudonimización HMAC-SHA256 de `id_usuario` antes de Bronze (puede haberse adelantado en Sprint 1, formalizar acá).
  - Controles de acceso por capa documentados.
  - Marco de cumplimiento Ley 1581 / Ley 1712.
- **Bonus 1:** Trino como tercer motor SQL leyendo las mismas tablas Gold (+2 pt).
- **Bonus 2:** `Makefile` orquestando el pipeline completo en un solo comando (+1 pt) — debería ya existir desde Sprint 0/1, formalizar y documentar.
- **Bonus 3:** Notebook `notebooks/03_eda_completo.ipynb` con análisis exploratorio sobre toda la capa Gold (+1 pt).

**MVP:** los notebooks de ML corren end-to-end, Trino lee Gold, y los ADRs están aprobados.

---

## Cómo trabajamos cada sprint (ritual)

1. **Kick-off (30 min):** revisar la guía `docs/sprints/sprint-N-*.md` correspondiente. Aclarar bloqueantes.
2. **Distribución de tareas:** cada sprint tiene 3-6 tareas grandes. Una persona asume cada una. Si alguna es muy grande, se subdivide.
3. **Trabajo paralelo:** ramas `feat/sprint-N-tarea-X`. PRs pequeños y revisados por al menos otro miembro.
4. **Demo interna (al final del sprint):** correr el MVP delante del equipo. Si pasa, merge a `main`.
5. **Retro corta (15 min):** qué funcionó, qué no, qué arrastramos al siguiente sprint.

---

## ¿Qué pasa si nos atrasamos?

Orden de **prioridad para no perder nota** (según los pesos típicos de un curso de SID):

1. **Sprint 1** (Lakehouse Medallion + Iceberg). Si esto no funciona, no hay proyecto.
2. **Sprint 2 + 3** (Streaming end-to-end). Sin streaming pierden el eje grande del curso.
3. **Sprint 4** (Decisiones documentadas). Los ADRs pesan en la nota porque demuestran ingeniería real.
4. **Sprint 5 — ML + Cloud**. Si toca recortar, los bonus se pueden dejar como nice-to-have.

Si vemos a mitad de camino que un sprint no cabe, **cortamos alcance, no calidad**. Mejor entregar 3 preguntas batch sólidas que 4 a medias.