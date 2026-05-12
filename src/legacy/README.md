# Legacy · MapReduce de incidentes (Módulo 01)

Este módulo demuestra el **paradigma MapReduce clásico** sobre el dataset
MEData de incidentes viales del Valle de Aburrá. Cumple el módulo 01
("Arqueología de datos") de la propuesta del curso.

## El problema histórico que reproduce

Hasta 2016, los CSVs históricos de MEData venían:

- **sin encabezados**,
- con columnas en orden distinto al actual,
- con `barrio_accidente` en lugar de `barrio`,
- sin columna `comuna`,
- con fechas en formato `dd/mm/yyyy hh:mm:ss`,
- con ubicación como `lon|lat` separada por pipe (sin corchetes).

Desde 2017 el portal estandarizó:

- 8 columnas en lugar de 7,
- fechas ISO 8601 (`yyyy-MM-ddTHH:mm:ss.sssZ`),
- columna `comuna` agregada,
- columna `location` como string `[lon, lat]`.

El job MapReduce debe **detectar el esquema por contenido** (formato de
fecha + número de columnas), normalizar, deduplicar por `nro_radicado` (un
registro puede aparecer en ambos archivos si el período de transición se
duplicó), y emitir un TSV con schema canónico ingerible a Bronze.

## Cómo correrlo end-to-end

```bash
# 1. Generar los dos CSV "legacy" desde el CSV unificado actual
python src/legacy/generar_dataset_legacy.py
# crea data/raw/medata_legacy/incidentes_pre2017.csv y _post2017.csv

# 2. Correr el job mrjob (modo inline, equivalente a stand-alone runner)
pip install mrjob
python src/legacy/mapreduce_incidentes.py \
    data/raw/medata_legacy/incidentes_pre2017.csv \
    data/raw/medata_legacy/incidentes_post2017.csv \
    --output-dir data/processed/incidentes_normalizados

# 3. Ingestar la salida a Bronze (Iceberg)
docker compose exec spark-iceberg python \
    /workspace/src/batch/bronze/ingest_legacy_mr.py

# 4. Verificar la tabla
docker compose exec spark-iceberg pyspark <<'EOF'
spark.sql("SELECT COUNT(*) FROM demo.pulsomed.bronze.medata_incidentes_legacy_mr").show()
spark.sql("""
  SELECT anio, COUNT(*) AS n
  FROM demo.pulsomed.bronze.medata_incidentes_legacy_mr
  GROUP BY anio ORDER BY anio
""").show(20, false)
EOF
```

Todo lo anterior está empaquetado en el Makefile como:

```bash
make legacy-generar      # paso 1
make legacy-mapreduce    # paso 2
make legacy-ingest       # paso 3
make pipeline-legacy     # 1 + 2 + 3 encadenados
```

## ¿Por qué mrjob y no Hadoop puro?

Trade-off documentado en el ADR informal del Sprint 4:

- **mrjob (la elegida)**: corre el job en modo `inline` (un proceso Python)
  o `local` (workers Python paralelos), y con `-r hadoop` despacha al
  cluster real cuando exista. Cubre el módulo 01 demostrando el paradigma
  (mapper + reducer + combiner opcional + counters) sin agregar 4 servicios
  al docker-compose (NameNode, DataNode, ResourceManager, NodeManager).
- **Hadoop Java puro**: más fiel al curso pero suma >1 GB de imagen y
  cuesta 1-2 días de trabajo en configuración WAR/JAR + paths HDFS, sin
  agregar capacidad analítica nueva. Reservado por si el evaluador lo pide
  explícitamente; el código mrjob es directamente convertible.

## Métricas que el job reporta

`mrjob` expone counters cuando se corre. Tras la corrida típica
(270 k filas reales partidas en pre/post-2017):

```
entrada
  esquema_pre2017:  ~95 000
  esquema_post2017: ~175 000
calidad
  fecha_no_reconocida: <50  (filas con FECHA_ACCIDENTES corrupta)
  sin_radicado:        0
  lineas_malformadas:  0
salida
  registros_emitidos:    ~270 000
  duplicados_resueltos:  0    (en este dataset; en MEData real >0 por transición)
```

## Archivos en este módulo

```
src/legacy/
  __init__.py
  README.md                       (este archivo)
  generar_dataset_legacy.py       (recrea el problema histórico)
  mapreduce_incidentes.py         (el job mrjob; salida = TSV canónico)

src/batch/bronze/
  ingest_legacy_mr.py             (TSV canónico → bronze.medata_incidentes_legacy_mr)
```

La tabla Iceberg resultante (`demo.pulsomed.bronze.medata_incidentes_legacy_mr`)
puede consumirse desde Silver/Gold como una fuente adicional, o validarse
contra `demo.pulsomed.bronze.medata_incidentes` (el camino "moderno"). Si
las dos tablas convergen en cardinalidad y dataset, el job MapReduce es
funcionalmente equivalente al PySpark ingest — eso es la **prueba** del
módulo 01.
