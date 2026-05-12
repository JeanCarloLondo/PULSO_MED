# ADR 05 · Delta Lake vs Apache Iceberg como table format

**Estado:** Aceptado
**Fecha:** 2026-05-12
**Decisores:** equipo Pulso Medellín (ST1630, EAFIT)
**Módulo del curso:** 05 — Lakehouse y formatos transaccionales

## Contexto

La propuesta de Pulso Medellín exige (sección 5) una capa Medallion
(Bronze→Silver→Gold) con metadata transaccional (ACID, time travel, schema
evolution) y especialmente **interoperabilidad multi-motor**: el mismo
warehouse debe ser legible desde Spark (lo que ya tenemos), futuro Trino
(Bonus 1 del Sprint 5), notebooks Colab/DuckDB para análisis remoto, y
eventualmente Snowflake/Athena como motores managed-cloud.

Los dos formatos candidatos en 2026 son **Delta Lake** y **Apache Iceberg**.
Ambos son open-source, ambos soportan ACID, schema evolution y time travel.
La decisión gira en torno a tres ejes:

1. ¿Quién puede leer las tablas además de Spark?
2. ¿Qué tan invasivo es el lock-in con un proveedor?
3. ¿Cuál se opera mejor con el catálogo que ya elegimos (REST)?

## Decisión

**Adoptamos Apache Iceberg** con catálogo REST (`tabulario/iceberg-rest`) y
warehouse físico en MinIO/S3 (`s3://warehouse/`). Sin Hive Metastore.

Tablas existentes:

```
demo.pulsomed.bronze.{medata_incidentes, metro_afluencia,
                      encicla_prestamos, siata_lecturas,
                      geomedellin_comunas, simm_aforos}
demo.pulsomed.silver.{incidentes_geocodificados, afluencia_horaria,
                      viajes_encicla_anonimizados, lecturas_aire_validas,
                      aforos_corredor_geo}
demo.pulsomed.gold.{afluencia_vs_pm25, accidentalidad_por_comuna,
                    demanda_encicla_vs_clima, corredores_riesgo_compuesto}
```

## Comparativa de los 4 consumidores objetivo

| Consumidor | Soporte Iceberg (2026) | Soporte Delta (2026) | Veredicto |
|------------|--------------------------|------------------------|-----------|
| **Databricks Runtime** | Lectura nativa (Iceberg REST + UniForm). Escritura mejorada en runtime 14+ | Nativo, formato original. Mejor soporte de optimizaciones (Photon, Liquid Clustering) | Tie técnico; Delta es "casa" pero Iceberg ya no penaliza |
| **Snowflake** | Iceberg tables nativas, escritura desde Snowflake hacia REST Catalog | Sólo lectura via "Delta Lake Direct" (managed table). Sin escritura push-down | **Iceberg gana** |
| **AWS Athena** | Iceberg engine v3 totalmente soportado (SELECT, UPDATE, MERGE, time travel) | Lectura, sin DML | **Iceberg gana** |
| **Colab / notebooks remotos (DuckDB, pyiceberg, deltalake)** | `pyiceberg` lee directo del REST catalog en S3, sin Spark | `deltalake` Python lee bien, pero requiere acceso filesystem (no catálogo) | **Iceberg gana** por el modelo REST |

El argumento central: **Iceberg gana en 3 de los 4 motores objetivo**, y donde
no gana (Databricks) tampoco pierde de forma significativa en 2026 gracias
a UniForm.

## Alternativas evaluadas

### A. Delta Lake con Hive Metastore

- **Pros**
  - Ecosystema rico en Databricks (origen del formato).
  - Liquid Clustering y Photon son ventajas de performance reales si la
    nube destino es Azure/Databricks.
- **Contras**
  - Hive Metastore agrega un servicio Thrift más al stack, con Java/PostgreSQL
    backing. En docker-compose son ~600 MB más y otra fuente de fallas.
  - Para que Snowflake/Athena lean tablas Delta hay que exportar manifestos
    Symlink — fricción innecesaria.
  - Acoplamiento implícito con la roadmap de Databricks (su propietario es
    Databricks Inc.).

### B. Apache Iceberg con catálogo REST (la elegida)

- **Pros**
  - Catálogo REST es el estándar de-facto para Iceberg (project-governance
    abierto, ASF).
  - La imagen `tabulario/iceberg-rest` no necesita base de datos persistente
    para el catálogo en modo demo (usa JDBC interno SQLite-like) → costo
    operativo bajo en docker-compose.
  - PyIceberg permite leer las tablas Gold desde **stream-runner** sin Spark:
    esto desbloquea la migración del `job_hibrido.py` a consultas en vivo
    (Sprint 4) sin agregar Spark al contenedor liviano.
  - Snapshot retention y branching (`main`, `dev`) son nativos y bien
    documentados.
- **Contras**
  - El catálogo REST en Tabulario es **stateless por defecto**: la metadata
    persiste en S3 pero la BD interna no — en producción hay que cambiar a
    JDBC catalog persistente. Documentado para Sprint 5.
  - Operaciones de mantenimiento (compaction, snapshot expiration) se hacen
    via `CALL system.rewrite_data_files` y `expire_snapshots` — no hay UI
    operativa equivalente a Databricks. Para un proyecto de curso es OK.

### C. Apache Iceberg con catálogo Hive

- Considerado y descartado por la misma razón que A: añade complejidad sin
  ganar nada que el REST no haga ya.

### D. Apache Iceberg con catálogo JDBC

- Más cercano a producción que REST stateless, pero requiere PostgreSQL en
  compose y no se justifica en una demo local. Se reserva como evolución
  para el ADR 07 (cloud).

## Consecuencias

### Lo que se vuelve más fácil

- **Multi-motor real**: Spark hoy, Trino mañana (Bonus 1 Sprint 5), DuckDB en
  un notebook desde Colab — todos pueden leer `s3://warehouse/` con el mismo
  REST URI. La promesa del lakehouse no es retórica, es ejecutable.
- **Time travel** y branching: el ranking de "alta siniestralidad" se publica
  en `gold.corredores_riesgo_compuesto` y mañana se puede consultar la versión
  de hace 6 meses con `VERSION AS OF` para comparar evolución.
- **Schema evolution** sin reescribir datos: cuando MEData publique 2025 con
  un campo adicional, `ALTER TABLE ... ADD COLUMN` no toca los Parquet del
  histórico.

### Lo que se vuelve más difícil

- Si el equipo se mueve mañana a un Databricks puro, "Delta era la opción
  natural" — habrá un costo de migración. UniForm reduce este costo a casi
  cero hoy, pero el reproche existirá.
- Operaciones de mantenimiento (compaction de archivos pequeños, expiración
  de snapshots) son **manuales** y requieren scripts periódicos. En Delta con
  Databricks es automático ("Auto Optimize"). Mitigación: añadiremos un job
  `make iceberg-compact` en Sprint 5 que aplique `rewrite_data_files` sobre
  todas las tablas Bronze (Bronze acumula muchos archivos pequeños por el
  append diario).

### Señales que indicarían revisar esta decisión

1. Si la organización destino estandariza en Databricks y todas las
   herramientas son Delta-native — el costo de mantener Iceberg superaría el
   beneficio multi-motor.
2. Si Snowflake/Athena retroceden en soporte Iceberg (improbable; los dos lo
   listan como roadmap "core" en 2026).
3. Si necesitamos **change data feed** (CDF) en producción para un consumer
   reactivo. Delta tiene CDF maduro hace años; Iceberg tiene un equivalente
   parcial con tablas changelog (más nuevo). Hoy no lo necesitamos.

## Implementación

- Configuración de la sesión Spark: `src/shared/config.py::crear_spark_session`
  con `spark.sql.catalog.demo.type=rest` y warehouse `s3://warehouse/`.
- Inicialización de namespaces: `scripts/init_iceberg_namespaces.py` (idempotente).
- Smoke test: `tests/smoke/test_iceberg.py` valida CREATE + INSERT + SELECT +
  DROP de una tabla Iceberg sobre MinIO.
- Bronze append: `src/shared/bronze_utils.py::escribir_bronze` aplica
  `partitionedBy(fecha_ingesta)` y `append()` idempotente.
- Silver/Gold: `createOrReplace()` con `partitionedBy(anio)` para particiones
  estables.

## ADRs relacionados

- ADR 02 (`02-lambda-vs-kappa.md`) — por qué tenemos un lado batch que
  necesita un table format transaccional.
- ADR 04 (`04-benchmark-formatos.md`) — qué codec de archivos usa Iceberg por
  debajo (Parquet+ZSTD).
- ADR 07 (`07-cloud-aws-vs-gcp.md`) — cómo se traduce esta decisión a un
  Glue/BigQuery/Snowflake en producción.
