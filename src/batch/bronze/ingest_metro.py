"""
ingest_metro.py — Bronze · Metro afluencia horaria por línea (2022-2024).

Fuente: portal de Datos Abiertos del Metro de Medellín (ArcGIS Hub),
descargado por `scripts/descargar_metro_afluencia_real.py` (Sprint 1.5).
Los xlsx anuales se convierten a CSV largo: una fila por (fecha × línea × hora).

Esquema CSV (formato largo, real):
    fecha (yyyy-MM-dd), linea (texto), hora (int 4..23), pasajeros (long)

Granularidad: la fuente pública NO desglosa por estación — sólo por línea.
Lo documentamos explícitamente (ADR Sprint 1.5) y respetamos ese límite.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_metro.py
"""

from __future__ import annotations

import glob
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion
from shared.config import TBL_BRONZE_METRO, crear_spark_session

PATRON = "/workspace/data/raw/metro_afluencia/afluencia_metro_*.csv"
FUENTE_ID = "metro_afluencia_real"


def main() -> int:
    log_seccion("Bronze · Metro afluencia horaria por línea (real)")

    rutas = sorted(glob.glob(PATRON))
    if not rutas:
        log_err(f"Sin archivos en {PATRON}")
        log_err("Correr antes:  make datos-reales")
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

    # Validar que el schema real está presente
    esperadas = {"fecha", "linea", "hora", "pasajeros"}
    faltantes = esperadas - set(df.columns)
    if faltantes:
        log_err(f"Columnas faltantes en CSV: {faltantes}")
        log_err(f"Columnas encontradas: {df.columns}")
        return 2

    log_ok(f"Filas raw consolidadas: {df.count():,}")

    df = (
        df.withColumn("fecha", F.to_date("fecha"))
          .withColumn("hora", F.col("hora").cast("int"))
          .withColumn("pasajeros", F.col("pasajeros").cast("long"))
          .withColumn("linea", F.trim(F.col("linea")))
          .filter(F.col("pasajeros") > 0)
          .filter(F.col("hora").between(0, 23))
    )

    n = escribir_bronze(
        spark, df, TBL_BRONZE_METRO, FUENTE_ID,
        nombre_archivo=";".join(Path(r).name for r in rutas),
    )
    log_ok(f"Bronze Metro listo: {n:,} filas (granularidad fecha×línea×hora)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
