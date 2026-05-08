"""
ingest_simm.py — Bronze · SIMM aforos vehiculares + datos de cámaras.

Fuentes (medata.gov.co):
  - aforos_vehiculares.csv  (~19 MB, conteos manuales por intersección)
  - simm_traffic_data.csv   (~816 MB, datos continuos de cámaras INDRA)

Para Sprint 1 cargamos AMBOS archivos a la misma tabla Bronze, con un
discriminante `tipo_aforo` ∈ {manual, camara}. Silver decide qué unificar
y qué dejar separado para B-4 (corredores de mayor riesgo).

Observación de tamaño: simm_traffic_data.csv es enorme (~3M filas). Para
el Sprint 1 podemos limitarlo con env var SIMM_LIMIT_FILAS si hace falta.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_simm.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import TBL_BRONZE_SIMM, crear_spark_session

CSV_AFOROS  = "/workspace/data/raw/simm_aforos/aforos_vehiculares.csv"
CSV_TRAFFIC = "/workspace/data/raw/simm_aforos/simm_traffic_data.csv"
FUENTE_ID = "simm"

LIMITE_FILAS = int(os.getenv("SIMM_LIMIT_FILAS", "300000"))


def _normalizar(c: str) -> str:
    return (
        c.lower()
         .replace("ñ", "n")
         .replace("á", "a").replace("é", "e").replace("í", "i")
         .replace("ó", "o").replace("ú", "u")
         .replace(" ", "_")
         .replace(".", "_")
         .replace("/", "_")
    )


def main() -> int:
    log_seccion("Bronze · SIMM aforos + tráfico cámaras")

    spark = crear_spark_session("Bronze-SIMM")
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Aforos manuales ──────────────────────────────────────────────
    p1 = Path(CSV_AFOROS)
    df_total = None

    if p1.exists() and p1.stat().st_size > 0:
        log_ok(f"Leyendo aforos manuales: {p1.name}")
        df1 = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(CSV_AFOROS)
        )
        for c in df1.columns:
            df1 = df1.withColumnRenamed(c, _normalizar(c))
        df1 = df1.withColumn("tipo_aforo", F.lit("manual"))
        log_ok(f"  filas: {df1.count():,}, cols: {len(df1.columns)}")
        df_total = df1
    else:
        log_warn(f"No encontrado: {CSV_AFOROS}")

    # ── 2. Cámaras INDRA (gigante; muestreado a LIMITE_FILAS) ───────────
    p2 = Path(CSV_TRAFFIC)
    if p2.exists() and p2.stat().st_size > 0:
        log_ok(f"Leyendo tráfico cámaras (limit={LIMITE_FILAS:,}): {p2.name}")
        df2 = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(CSV_TRAFFIC)
            .limit(LIMITE_FILAS)
        )
        for c in df2.columns:
            df2 = df2.withColumnRenamed(c, _normalizar(c))
        df2 = df2.withColumn("tipo_aforo", F.lit("camara"))
        log_ok(f"  filas: {df2.count():,}, cols: {len(df2.columns)}")

        # Las dos fuentes tienen esquemas distintos; las dejamos como tablas
        # paralelas dentro de la MISMA tabla Bronze usando unionByName(allowMissingColumns=True).
        if df_total is None:
            df_total = df2
        else:
            df_total = df_total.unionByName(df2, allowMissingColumns=True)
    else:
        log_warn(f"No encontrado: {CSV_TRAFFIC}")

    if df_total is None:
        log_err("Ningún archivo SIMM disponible.")
        return 1

    n = escribir_bronze(
        spark, df_total, TBL_BRONZE_SIMM, FUENTE_ID,
        nombre_archivo=f"{p1.name};{p2.name}",
    )
    log_ok(f"Bronze SIMM listo: {n:,} filas (manual+camara)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
