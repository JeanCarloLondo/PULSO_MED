"""
ingest_legacy_mr.py — Bronze · ingesta de la salida normalizada del MapReduce
legacy (src/legacy/mapreduce_incidentes.py).

Demuestra el cierre del ciclo del Módulo 01 (Arqueología de datos):
    CSV viejo + CSV nuevo  →  MapReduce  →  TSV unificado  →  Bronze Iceberg

Tabla destino:
    demo.pulsomed.bronze.medata_incidentes_legacy_mr

Esquema esperado en el TSV (sin encabezado, separador \\t):
    nro_radicado, fecha, anio, mes, clase, gravedad, barrio, comuna,
    direccion, longitud, latitud

Ejecutar:
    docker compose exec -T spark-iceberg python \\
        /workspace/src/batch/bronze/ingest_legacy_mr.py
"""

from __future__ import annotations

import glob
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion
from shared.config import CATALOG, NS_BRONZE, crear_spark_session

TABLA = f"{CATALOG}.{NS_BRONZE}.medata_incidentes_legacy_mr"
PATRON_TSV = "/workspace/data/processed/incidentes_normalizados/part-*"
FUENTE_ID = "medata_legacy_mr"

ESQUEMA = StructType([
    StructField("nro_radicado", StringType(), False),
    StructField("fecha", StringType(), False),
    StructField("anio", IntegerType(), False),
    StructField("mes", IntegerType(), False),
    StructField("clase", StringType(), True),
    StructField("gravedad", StringType(), True),
    StructField("barrio", StringType(), True),
    StructField("comuna", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("longitud", StringType(), True),
    StructField("latitud", StringType(), True),
])


def main() -> int:
    log_seccion("Bronze · MEData legacy normalizado (MapReduce)")

    rutas = sorted(glob.glob(PATRON_TSV))
    if not rutas:
        log_err(f"No hay TSVs en {PATRON_TSV}")
        log_err("Correr primero el job mrjob:")
        log_err("  python /workspace/src/legacy/mapreduce_incidentes.py \\")
        log_err("    /workspace/data/raw/medata_legacy/incidentes_pre2017.csv \\")
        log_err("    /workspace/data/raw/medata_legacy/incidentes_post2017.csv \\")
        log_err("    --output-dir /workspace/data/processed/incidentes_normalizados")
        return 1

    log_ok(f"Archivos TSV: {len(rutas)}")

    spark = crear_spark_session("Bronze-Legacy-MR")
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .schema(ESQUEMA)
        .csv(rutas)
        .withColumn("longitud", F.col("longitud").cast("double"))
        .withColumn("latitud", F.col("latitud").cast("double"))
    )
    log_ok(f"Filas: {df.count():,}")

    n = escribir_bronze(
        spark, df, TABLA, FUENTE_ID,
        nombre_archivo=";".join(Path(r).name for r in rutas),
    )
    log_ok(f"{TABLA}: {n:,} filas (salida del MapReduce ingestada a Bronze)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
