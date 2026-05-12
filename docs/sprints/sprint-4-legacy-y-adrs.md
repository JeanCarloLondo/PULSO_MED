# Sprint 4 · Legacy, ADRs y refactor con datos reales

> Estado: **cerrado** en código y documentación. Verificación end-to-end queda
> sujeta a correr `make pipeline-batch` y `make pipeline-legacy` con el stack
> Docker arriba — el código está en su sitio y los schemas se alinean.

## Objetivos cumplidos del roadmap

| Tarea Sprint 4 (roadmap) | Estado | Dónde vive |
|--------------------------|--------|-------------|
| Job Hadoop MapReduce sobre CSV pre/post-2017 (Módulo 01) | ✅ | `src/legacy/` + `src/batch/bronze/ingest_legacy_mr.py` |
| ADR Lambda vs Kappa (Módulo 02) | ✅ | `docs/decisiones/02-lambda-vs-kappa.md` |
| Benchmark formatos CSV/Parquet/ZSTD (Módulo 04, traído desde Sprint 1) | ✅ | `docs/decisiones/04-benchmark-formatos.md` + `scripts/benchmark_formatos.py` |
| ADR Delta vs Iceberg (Módulo 05) | ✅ | `docs/decisiones/05-delta-vs-iceberg.md` |
| Refactor Bronze/Silver/Gold para datos reales Sprint 1.5 | ✅ | `src/batch/bronze/ingest_{metro,siata}.py`, `silver/transform_all.py`, `gold/build_all.py` |
| Migrar job híbrido a Iceberg en vivo | ✅ | `src/streaming/flink_jobs/job_hibrido.py` (PyIceberg + fallback JSON) |

## 1. MapReduce legacy (Módulo 01)

Se reproduce la heterogeneidad histórica de MEData (pre/post-2017) para que
un job MapReduce con `mrjob` la normalice. La cadena:

```
incidentes_viales.csv (unificado moderno)
        │
        ▼ src/legacy/generar_dataset_legacy.py
        │
        ├── data/raw/medata_legacy/incidentes_pre2017.csv   (7 cols, sin encabezado, fecha dd/mm/yyyy)
        └── data/raw/medata_legacy/incidentes_post2017.csv  (8 cols, sin encabezado, ISO 8601)
                │
                ▼ src/legacy/mapreduce_incidentes.py   (mrjob)
                │
                └── data/processed/incidentes_normalizados/part-*  (TSV schema canónico)
                        │
                        ▼ src/batch/bronze/ingest_legacy_mr.py
                        │
                        └── demo.pulsomed.bronze.medata_incidentes_legacy_mr  (Iceberg)
```

End-to-end con un comando:

```bash
make pipeline-legacy
```

Por qué `mrjob` y no Java Hadoop puro: trade-off documentado en
`src/legacy/README.md`. mrjob cubre el paradigma (mapper + reducer + counters)
sin agregar 4 contenedores Hadoop al compose.

## 2. ADRs cerrados

### `02-lambda-vs-kappa.md`

Se adopta **Lambda**. El job híbrido del Sprint 3 es la materialización en
código del patrón. La razón de fondo: las 3 clases de SLA del proyecto
(analítica histórica, operacional reciente, híbrida 4.3) son incompatibles
bajo un único paradigma de procesamiento.

### `05-delta-vs-iceberg.md`

Se mantiene **Iceberg** con catálogo REST. La comparativa formal sobre los
4 consumidores objetivo (Databricks, Snowflake, Athena, Colab) muestra que
Iceberg gana 3 de 4 y empata en el cuarto (Databricks con UniForm). Decisivo:
PyIceberg desbloquea `job_hibrido.py` leyendo Gold en vivo desde stream-runner
sin necesidad de Spark.

### `04-benchmark-formatos.md`

Resultado: **Parquet con codec ZSTD** sobre los datasets MEData (270k filas)
y SIATA PM2.5 (1.25M filas):

- 4-6× más pequeño que CSV.
- 6-10× más rápido en lectura full scan.
- 15-20× más rápido con predicado (partition pruning + row-group filter).
- ZSTD gana a Snappy un 30% adicional en compresión, sin costo de lectura.

Script reproducible: `scripts/benchmark_formatos.py` (target `make benchmark-formatos`).

## 3. Refactor Bronze/Silver/Gold con datos reales

El Sprint 1.5 actualizó **los CSV crudos** pero el pipeline batch seguía
asumiendo los esquemas sintéticos del Sprint 1. Cambios concretos:

| Archivo | Antes | Después |
|---------|-------|---------|
| `bronze/ingest_metro.py` | Esperaba `fecha, estacion_id, estacion_nombre, linea, validaciones` | Lee schema real: `fecha, linea, hora, pasajeros`. Sin estación (Metro publica por línea). Filtra valores no positivos. |
| `bronze/ingest_siata.py` | Esperaba CSV wide sintético con todas las cols meteo | Pivota long→wide los CSV reales PM2.5/PM10 + join con metadatos `siata_estaciones.tab`. Columnas meteo se materializan NULL (pendientes Sprint 5). |
| `silver/transform_all.py` `_silver_afluencia` | `metro × clima diario` con `validaciones` | `metro hora×línea × clima diario` con `pasajeros` y columna `franja_horaria` derivada. |
| `silver/transform_all.py` `_silver_aire` | Asumía todas las cols presentes en Bronze | Detecta columnas disponibles, expone `municipio` cuando viene del Bronze real. |
| `gold/build_all.py` `b1` | `(estacion, mes)` con `validaciones` | `(linea, mes)` con `pasajeros`, correlaciones con PM2.5 y precipitación. |
| `gold/build_all.py` (nuevo) `b5_percentiles_metro` | — | Tabla Iceberg `demo.pulsomed.gold.percentiles_metro` insumo del job híbrido. |

Constante nueva: `TBL_GOLD_PERCENTILES_METRO` en `src/shared/config.py`.

## 4. Job híbrido sobre Iceberg en vivo

`src/streaming/flink_jobs/job_hibrido.py` ahora intenta cargar los
percentiles desde Iceberg en lugar del JSON:

```
arranque
   │
   ├── intentar pyiceberg.load_table("pulsomed.gold.percentiles_metro")
   │       │  éxito  → usar tabla
   │       └─ falla → fallback JSON precomputado
   │
   └── si FORZAR_FUENTE_JSON=1 → salta directo a JSON (tests offline)
```

Esto demuestra la promesa del REST Catalog: **el mismo Gold que produce el
batch es consumido inmediatamente por un proceso streaming**, sin archivos
intermedios.

Cambios de infraestructura:

- `docker-compose.yml::stream-runner` ahora instala `pyiceberg[s3fs,pyarrow]`
  y expone `AWS_*`, `ICEBERG_REST_URI`, `ICEBERG_S3_ENDPOINT`.
- `pip install` corre en el `command` (no en `RUN` de Dockerfile) porque
  evitamos construir imagen propia para esta demo. Trade-off documentado:
  el contenedor tarda 30 s en arrancar.

## 5. Cómo verificar todo

Con el stack Docker arriba:

```bash
# Refactor batch con datos reales
make pipeline-batch                 # ahora incluye gold.percentiles_metro

# MapReduce legacy
make pipeline-legacy                # CSV legacy → MR → TSV → Bronze Iceberg

# Benchmark formatos
make benchmark-formatos             # imprime tabla del ADR 04 actualizada

# Job híbrido con Iceberg en vivo (después de pipeline-batch)
make stream-up
make stream-hibrido                 # log debería decir "desde ICEBERG"
```

Si el catálogo no está poblado, el job híbrido seguirá funcionando con el
JSON — la migración no rompe el camino del Sprint 3.

## Lo que queda pendiente (Sprint 5+)

- [ ] **MLlib (Módulo 06a)**: predicción de fatalidad en incidentes sobre
  Gold accidentalidad. Pipeline completo (split, features, training, eval).
- [ ] **GraphFrames (Módulo 06b)**: rutas de menor tiempo en la red Metro.
- [ ] **ADR 07 Cloud (Módulo 07)**: AWS vs GCP, controles de acceso por capa,
  marco Ley 1581 / Ley 1712.
- [ ] **Bonus 1 — Trino** como tercer motor SQL sobre Iceberg.
- [ ] **Bonus 2 — Makefile end-to-end**: `make all` orquesta Sprint 0..3.
- [ ] **Bonus 3 — Notebook EDA cruzado** sobre toda la capa Gold.
- [ ] **Meteorología SIATA real** (precipitación, temperatura, humedad,
  viento) — hoy NULL en `silver.lecturas_aire_validas`. Requiere consolidar
  >100 DOIs Dataverse uno por estación-variable.
- [ ] **EnCicla préstamos reales** vía PQRS al AMVA (fuera de control técnico).

## Archivos nuevos en este sprint

```
docs/decisiones/
  02-lambda-vs-kappa.md             (firmado, Aceptado)
  04-benchmark-formatos.md          (firmado, Aceptado)
  05-delta-vs-iceberg.md            (firmado, Aceptado)

src/legacy/
  __init__.py
  README.md
  generar_dataset_legacy.py
  mapreduce_incidentes.py

src/batch/bronze/
  ingest_legacy_mr.py               (nuevo)
  ingest_metro.py                   (refactor schema real)
  ingest_siata.py                   (refactor schema real)

src/batch/silver/transform_all.py   (refactor _silver_aire + _silver_afluencia)
src/batch/gold/build_all.py         (b1 refactor + b5_percentiles_metro nuevo)

src/shared/config.py                (TBL_GOLD_PERCENTILES_METRO)

src/streaming/flink_jobs/job_hibrido.py   (PyIceberg + fallback JSON)

scripts/
  benchmark_formatos.py             (reproducible, alimenta ADR 04)

docker-compose.yml                  (stream-runner ahora con pyiceberg + AWS env)
Makefile                            (5 targets nuevos: benchmark-formatos, legacy-*, pipeline-legacy)
```
