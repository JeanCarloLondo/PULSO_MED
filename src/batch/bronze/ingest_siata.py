"""
ingest_siata.py — Bronze · SIATA lecturas horarias PM2.5/PM10/clima.

Fuente real: Dataverse SIATA, DOI 10.83041/AUWZWT (155+ archivos .tab).
Fuente actual (Sprint 1): muestra sintética generada por
scripts/generar_muestras_sinteticas.py — la descarga real requiere `jq`
y los .tab requieren un parser adicional. El CSV sintético tiene el mismo
esquema lógico que se usará en streaming (Sprint 2).

Esquema:
    estacion_id, estacion_nombre, zona, latitud, longitud,
    timestamp, pm25, pm10, temperatura_c, humedad_pct,
    precipitacion_mm, viento_kmh

Convención: SIATA usa -999 como centinela de nulo (constante en config.py).
A Bronze llega tal cual; Silver lo convierte a NULL.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_siata.py
"""

from __future__ import annotations

import glob
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import TBL_BRONZE_SIATA, crear_spark_session

PATRON = "/workspace/data/raw/siata_historico/siata_pm25_*.csv"
FUENTE_ID = "siata_historico"


def main() -> int:
    log_seccion("Bronze · SIATA lecturas históricas")

    rutas = sorted(glob.glob(PATRON))
    if not rutas:
        log_err(f"Sin archivos en {PATRON}")
        return 1
    log_ok(f"Archivos: {len(rutas)}")

    spark = crear_spark_session("Bronze-SIATA")
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(rutas)
    )

    log_ok(f"Filas raw: {df.count():,}")

    df = (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
          .withColumn("pm25", F.col("pm25").cast("double"))
          .withColumn("pm10", F.col("pm10").cast("double"))
    )

    n = escribir_bronze(
        spark, df, TBL_BRONZE_SIATA, FUENTE_ID,
        nombre_archivo=";".join(Path(r).name for r in rutas),
    )
    log_ok(f"Bronze SIATA listo: {n:,} filas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
