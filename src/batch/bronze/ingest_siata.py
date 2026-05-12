"""
ingest_siata.py — Bronze · SIATA lecturas horarias PM2.5/PM10 (datos REALES).

Fuente: Dataverse SIATA, descargada por `scripts/descargar_siata_real.py`
(Sprint 1.5). Tres archivos:

    siata_pm25_horario.csv     timestamp, estacion_id, variable, valor
    siata_pm10_horario.csv     idem
    siata_estaciones.tab       Código, Nombre_Completo_Estacion,
                               Nombre_Corto_Estacion, Latitud, Longitud, Municipio

Transformaciones aplicadas en Bronze:
    1. Pivot interno (long → wide): pm25 y pm10 quedan como columnas.
    2. Join con metadatos de estaciones para anexar lat/lon/nombre/municipio.
    3. Columnas meteorológicas (temperatura_c, humedad_pct, precipitacion_mm,
       viento_kmh) se materializan como NULL — la fuente real las publica en
       >100 DOIs por estación y se posterga a Sprint 5. Silver tolera NULL.

Esquema Bronze resultante:
    estacion_id, estacion_nombre, zona, latitud, longitud, municipio,
    timestamp, pm25, pm10,
    temperatura_c (null), humedad_pct (null),
    precipitacion_mm (null), viento_kmh (null),
    + columnas de auditoría inyectadas por bronze_utils.

Convención: SIATA codifica nulos con valor -999.0 (constante SIATA_NULL_SENTINEL).
Bronze mantiene el sentinel — Silver lo convierte a NULL real.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_siata.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import TBL_BRONZE_SIATA, crear_spark_session

RAIZ_REAL = "/workspace/data/raw/siata_historico"
ARCH_PM25 = f"{RAIZ_REAL}/siata_pm25_horario.csv"
ARCH_PM10 = f"{RAIZ_REAL}/siata_pm10_horario.csv"
ARCH_ESTACIONES = f"{RAIZ_REAL}/siata_estaciones.tab"
FUENTE_ID = "siata_real"


def _cargar_long(spark: SparkSession, ruta: str, nombre: str) -> DataFrame:
    """Carga un CSV largo (timestamp, estacion_id, variable, valor)."""
    if not Path(ruta).exists():
        log_warn(f"No existe {ruta}")
        return None
    df = (
        spark.read
        .option("header", "true")
        .csv(ruta)
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("valor", F.col("valor").cast(DoubleType()))
        .filter(F.col("timestamp").isNotNull())
    )
    log_ok(f"{nombre}: {df.count():,} filas largas")
    return df


def _cargar_estaciones(spark: SparkSession, ruta: str) -> DataFrame:
    """Carga el TAB de metadatos de estaciones SIATA."""
    if not Path(ruta).exists():
        log_warn(f"No existe {ruta} — coords serán NULL")
        return None
    esquema = StructType([
        StructField("codigo", StringType(), True),
        StructField("nombre_completo", StringType(), True),
        StructField("nombre_corto", StringType(), True),
        StructField("latitud", DoubleType(), True),
        StructField("longitud", DoubleType(), True),
        StructField("municipio", StringType(), True),
    ])
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .option("quote", '"')
        .schema(esquema)
        .csv(ruta)
        # nombre_corto es el id "BAR-TORR", "MED-UNFM", etc., que matcha estacion_id
        .select(
            F.col("nombre_corto").alias("estacion_id"),
            F.col("nombre_completo").alias("estacion_nombre"),
            "latitud", "longitud", "municipio",
        )
        .filter(F.col("estacion_id").isNotNull())
    )
    log_ok(f"Estaciones SIATA: {df.count()} con coordenadas")
    return df


def main() -> int:
    log_seccion("Bronze · SIATA real (PM2.5 + PM10 + metadatos)")

    spark = crear_spark_session("Bronze-SIATA")
    spark.sparkContext.setLogLevel("WARN")

    pm25 = _cargar_long(spark, ARCH_PM25, "PM2.5")
    pm10 = _cargar_long(spark, ARCH_PM10, "PM10")
    if pm25 is None and pm10 is None:
        log_err("No hay archivos SIATA reales. Correr `make datos-reales`.")
        return 1

    # Unir y pivotar long→wide
    df_long = pm25 if pm25 is not None else pm10
    if pm25 is not None and pm10 is not None:
        df_long = pm25.unionByName(pm10)

    wide = (
        df_long
        .groupBy("timestamp", "estacion_id")
        .pivot("variable", ["pm25", "pm10"])
        .agg(F.avg("valor"))
    )

    # Anexar metadatos de estación
    estaciones = _cargar_estaciones(spark, ARCH_ESTACIONES)
    if estaciones is not None:
        wide = wide.join(estaciones, on="estacion_id", how="left")
    else:
        for c in ("estacion_nombre", "latitud", "longitud", "municipio"):
            wide = wide.withColumn(c, F.lit(None).cast("string" if c != "latitud" and c != "longitud" else "double"))

    # Zona derivada del municipio (heurística simple, sin GeoJoin para Bronze)
    wide = wide.withColumn(
        "zona",
        F.when(F.col("municipio") == "Medellín", F.lit("valle_aburra_centro"))
         .when(F.col("municipio").isin("Bello", "Copacabana", "Girardota", "Barbosa"), F.lit("valle_aburra_norte"))
         .when(F.col("municipio").isin("Itagüí", "Envigado", "Sabaneta", "La Estrella", "Caldas"), F.lit("valle_aburra_sur"))
         .otherwise(F.lit("valle_aburra_centro")),
    )

    # Columnas meteorológicas inexistentes en la fuente real → NULL explícito
    for col_meteo in ("temperatura_c", "humedad_pct", "precipitacion_mm", "viento_kmh"):
        wide = wide.withColumn(col_meteo, F.lit(None).cast("double"))

    final = wide.select(
        "estacion_id", "estacion_nombre", "zona", "latitud", "longitud", "municipio",
        "timestamp", "pm25", "pm10",
        "temperatura_c", "humedad_pct", "precipitacion_mm", "viento_kmh",
    )

    log_ok(f"Bronze candidato: {final.count():,} filas (timestamp × estacion, wide)")

    nombres_archivos = []
    for f in (ARCH_PM25, ARCH_PM10, ARCH_ESTACIONES):
        if Path(f).exists():
            nombres_archivos.append(Path(f).name)

    n = escribir_bronze(
        spark, final, TBL_BRONZE_SIATA, FUENTE_ID,
        nombre_archivo=";".join(nombres_archivos),
    )
    log_ok(f"Bronze SIATA listo: {n:,} filas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
