"""
ingest_metro.py — Bronze · Metro afluencia diaria por estación (2022-2024).

Fuente real: ArcGIS Hub Metro de Medellín (xlsx anuales).
Fuente actual (Sprint 1): muestra sintética generada por
scripts/generar_muestras_sinteticas.py — ArcGIS Hub bloquea descargas
directas con 403; el archivo CSV cumple el mismo esquema lógico.

Esquema CSV: fecha, estacion_id, estacion_nombre, linea, validaciones

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_metro.py
"""

from __future__ import annotations

import glob
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import TBL_BRONZE_METRO, crear_spark_session

PATRON = "/workspace/data/raw/metro_afluencia/afluencia_metro_*.csv"
FUENTE_ID = "metro_afluencia"


def main() -> int:
    log_seccion("Bronze · Metro afluencia diaria")

    rutas = sorted(glob.glob(PATRON))
    if not rutas:
        log_err(f"Sin archivos en {PATRON}")
        return 1

    log_ok(f"Archivos a procesar: {len(rutas)}")
    for r in rutas:
        log_ok(f"  - {Path(r).name}")

    spark = crear_spark_session("Bronze-Metro")
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(rutas)
    )

    log_ok(f"Filas raw consolidadas: {df.count():,}")

    # Forzamos tipos consistentes incluso si una de las muestras venía con int
    df = (
        df.withColumn("fecha", F.to_date("fecha"))
          .withColumn("validaciones", F.col("validaciones").cast("long"))
    )

    n = escribir_bronze(
        spark, df, TBL_BRONZE_METRO, FUENTE_ID,
        nombre_archivo=";".join(Path(r).name for r in rutas),
    )
    log_ok(f"Bronze Metro listo: {n:,} filas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
