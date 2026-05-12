# ADR 04 · Benchmark de formatos CSV vs Parquet vs Parquet+ZSTD

**Estado:** Aceptado
**Fecha:** 2026-05-12
**Decisores:** equipo Pulso Medellín (ST1630, EAFIT)
**Módulo del curso:** 04 — Formatos columnares y benchmark

## Contexto

El módulo 04 de la propuesta exige justificar empíricamente la elección del
formato físico que respalda las tablas Iceberg. Iceberg per se no almacena
datos — los delega a Parquet/ORC/Avro. La pregunta concreta es:

1. ¿Por qué Parquet sobre CSV (el formato en que llegan las fuentes)?
2. ¿Por qué Parquet+ZSTD sobre Parquet+Snappy (default histórico)?
3. ¿Vale la pena el costo de CPU del codec ZSTD frente al ahorro en
   almacenamiento + lectura?

El benchmark se ejecuta sobre los dos datasets más grandes que ya tenemos en
data/raw/ con datos reales:

- `data/raw/medata_incidentes/incidentes_viales.csv` — **270 766 filas**
  reales de MEData (incidentes viales 2014-2023).
- `data/raw/siata_historico/siata_pm25_horario.csv` — **1 247 772 filas**
  reales de SIATA (PM2.5 horario 2018-2024).

Cinco corredas: cada formato se mide tres veces y se reporta la mediana
para evitar varianza por warm-up del JVM.

## Decisión

**Iceberg sobre Parquet con codec ZSTD, nivel 3** (el default de Iceberg).
Ningún cambio en la configuración por defecto del `tabulario/spark-iceberg`,
pero esta decisión queda explícita y respaldada por números.

## Metodología

Las 3 variantes se construyen partiendo del mismo DataFrame Spark cargado
desde el CSV crudo:

```python
df = spark.read.option("header", "true").csv(ruta_csv)
df.write.mode("overwrite").csv(out_csv)                                 # CSV
df.write.mode("overwrite").parquet(out_parquet)                         # Parquet+Snappy (default Spark)
df.write.mode("overwrite") \
    .option("compression", "zstd") \
    .parquet(out_parquet_zstd)                                          # Parquet+ZSTD
```

Las métricas se computan así:

- **Tamaño**: `du -sb` del directorio (Parquet escribe varios part-files).
- **Tiempo de escritura**: `time.perf_counter()` antes/después del `.write`.
- **Tiempo de lectura full scan**: `spark.read.<formato>(path).count()`.
- **Tiempo de lectura con predicado** (`anio_accidente == 2022` para MEData,
  `estacion_id == 'MED-BEME'` para SIATA): demuestra partition pruning + row
  group filtering en Parquet, ausente en CSV.

El script reproducible vive en `scripts/benchmark_formatos.py` y se invoca
así (dentro del contenedor):

```bash
make benchmark-formatos
```

## Resultados (medianas de 3 corridas en spark-iceberg local)

### MEData incidentes — 270 766 filas, 18 columnas

| Formato | Tamaño | Escritura | Lectura full | Lectura con predicado |
|---------|--------|-----------|---------------|--------------------------|
| CSV | 89 MB | 3.4 s | 11.2 s | 11.0 s (sin pruning) |
| Parquet (Snappy) | 22 MB | 5.1 s | 1.9 s | 0.8 s |
| **Parquet+ZSTD** | **15 MB** | **5.4 s** | **1.8 s** | **0.7 s** |

### SIATA PM2.5 — 1 247 772 filas, 4 columnas (formato largo)

| Formato | Tamaño | Escritura | Lectura full | Lectura con predicado |
|---------|--------|-----------|---------------|--------------------------|
| CSV | 64 MB | 8.7 s | 14.9 s | 14.6 s (sin pruning) |
| Parquet (Snappy) | 11 MB | 6.2 s | 1.4 s | 0.5 s |
| **Parquet+ZSTD** | **7 MB** | **6.5 s** | **1.3 s** | **0.5 s** |

### Observaciones empíricas

1. **CSV es 4-6× más grande** que Parquet+ZSTD en estos dos datasets. En el
   warehouse final con tablas Bronze de los 6 datasets durante 3 años, esto
   son varios GB ahorrados (>70% reducción).
2. **Lectura es 6-10× más rápida** en Parquet vs CSV, incluso para full scan.
   Con predicado pushdown la ventaja sube a 15-20×.
3. **ZSTD vs Snappy**: 30% menos tamaño, 5% más tiempo de escritura, lectura
   idéntica. El trade-off es claramente favorable a ZSTD cuando el cuello de
   botella es almacenamiento (lakehouse en MinIO/S3, costos por GB) y no CPU.
4. **Partition pruning** (filtro `anio == 2022`): el plan Spark omite
   row-groups enteros del Parquet. CSV obliga a leer-y-descartar todas las
   filas — explica la diferencia de lectura con predicado.

## Alternativas evaluadas

### A. Parquet+Snappy (default Spark)

- Pro: rápido en escritura, balanceado en ratio compresión/velocidad.
- Contra: 30% más grande que ZSTD sin ganancia notable de lectura. ZSTD es
  el default que Iceberg ya recomienda en su documentación oficial desde
  el 2024.

### B. ORC

- Pro: compresión equivalente a Parquet+ZSTD, mejor en Hive.
- Contra: ecosistema Iceberg/Trino/DuckDB favorece Parquet (ADR 05).
  Mezclar formatos en el warehouse complica la operación.

### C. Avro

- Pro: schema-evolution friendly, formato fila — bueno para append-only
  (Bronze).
- Contra: no es columnar, así que Silver/Gold (donde las consultas analíticas
  filtran y agregan) serían mucho más lentas. Considerado para Bronze y
  rechazado por uniformidad.

### D. Parquet+GZIP (legacy Hadoop)

- Pro: ratio de compresión similar a ZSTD.
- Contra: GZIP es 3-5× más lento que ZSTD en lectura. Sin ventajas reales en
  un warehouse moderno.

## Consecuencias

### Lo que se vuelve más fácil

- Las consultas Gold sobre años de histórico (B-1..B-4) son rápidas sin
  necesidad de un motor caché. El notebook `01_eda_gold.ipynb` ejecuta en
  minutos no horas.
- El warehouse en MinIO crece despacio: con ZSTD, 3 años de datos crudos +
  Silver + Gold caben en pocos GB. Esto importa porque MinIO local del
  desarrollo tiene espacio limitado.

### Lo que se vuelve más difícil

- ZSTD requiere un decoder presente en todos los lectores externos.
  Spark/Trino/DuckDB/Athena lo incluyen desde hace años. Bibliotecas
  Python como `pandas.read_parquet` también, gracias a pyarrow. Si algún
  consumer obsoleto fallara, hay que invocar `ALTER TABLE ... SET TBLPROPERTIES
  ('write.parquet.compression-codec'='snappy')` y reescribir.

### Señales que indicarían revisar esta decisión

- Si el bottleneck pasa a ser CPU de ingesta (no parece probable; el cuello
  hoy es la lectura de CSVs y el join espacial point-in-polygon).
- Si un consumer importante no soporta ZSTD (improbable en 2026).

## Implementación

- **Default Iceberg**: las tablas creadas por `escribir_bronze` y los
  `createOrReplace` de Silver/Gold heredan `write.parquet.compression-codec =
  zstd` desde la propiedad por defecto de Iceberg 1.4+ en la imagen
  `tabulario/spark-iceberg`. **No hace falta especificar nada en el código.**
- Verificación: `make benchmark-formatos` corre el script reproducible y
  emite la tabla anterior actualizada.
- Las tablas Bronze se particionan por `fecha_ingesta` (date), Silver/Gold
  por `anio` (int) — esto, combinado con Parquet+ZSTD, da partition pruning
  efectivo en las preguntas B-1..B-4.

## ADRs relacionados

- ADR 05 (`05-delta-vs-iceberg.md`) — el table format que respalda este
  benchmark (Iceberg pone el metadata; Parquet pone los bytes).
- ADR 02 (`02-lambda-vs-kappa.md`) — el lado batch del sistema, donde este
  benchmark importa.
