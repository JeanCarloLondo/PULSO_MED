"""
ingest_medata.py — Bronze · MEData incidentes viales 2014-2024.

Fuente: data/raw/medata_incidentes/incidentes_viales.csv  (medata.gov.co)

Decisión: a Bronze llega TODO sin transformar (incluso columnas en español
con tildes y nombres de campo como "AÑO" o "GRAVEDAD_ACCIDENTE"). Las
correcciones — coordenadas invertidas pre-2017, casteo de fechas, etc. —
se hacen en Silver.

Particionado: por `fecha_ingesta` (no por año del incidente, para que
re-ingestar no requiera escribir en particiones antiguas).

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_medata.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import TBL_BRONZE_MEDATA, crear_spark_session

CSV = "/workspace/data/raw/medata_incidentes/incidentes_viales.csv"
FUENTE_ID = "medata_incidentes"


def main() -> int:
    log_seccion("Bronze · MEData incidentes viales")

    p = Path(CSV)
    if not p.exists() or p.stat().st_size == 0:
        log_err(f"No encontrado: {CSV}")
        return 1

    spark = crear_spark_session("Bronze-MEData")
    spark.sparkContext.setLogLevel("WARN")

    # MEData usa header con tildes; lo dejamos tal cual y casteamos todo a string.
    # Silver es quien decide qué casteos hacer (FECHA_ACCIDENTES → timestamp, etc).
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(CSV)
    )

    cols = df.columns
    log_ok(f"Columnas leídas: {len(cols)}")
    log_ok(f"Filas raw: {df.count():,}")

    # Renombrar columnas para que sean válidas como nombres Iceberg
    # (sin tildes, espacios → underscore, mayúscula → minúscula).
    def normalizar(c: str) -> str:
        return (
            c.lower()
             .replace("ñ", "n")
             .replace("á", "a").replace("é", "e").replace("í", "i")
             .replace("ó", "o").replace("ú", "u")
             .replace(" ", "_")
        )

    for c in cols:
        df = df.withColumnRenamed(c, normalizar(c))

    n = escribir_bronze(spark, df, TBL_BRONZE_MEDATA, FUENTE_ID, p.name)
    log_ok(f"Bronze MEData listo: {n:,} filas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
