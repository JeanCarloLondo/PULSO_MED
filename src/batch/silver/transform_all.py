"""
transform_all.py — Bronze → Silver para las 5 entidades del Sprint 1.

Capa Silver: limpieza + casting + joins espaciales y temporales.
Cada entidad se construye en una función `_silver_<nombre>` y se escribe a
su tabla Iceberg correspondiente con `createOrReplace` (Silver es derivable
desde Bronze, así que reemplazamos siempre — idempotente).

Salidas:
    silver.incidentes_geocodificados   (MEData + GeoMedellín, coords corregidas)
    silver.afluencia_horaria           (Metro diario expandido a horario + clima)
    silver.viajes_encicla_anonimizados (préstamos + duración + categoría)
    silver.lecturas_aire_validas       (SIATA con -999 → NULL)
    silver.aforos_corredor_geo         (SIMM con corredor canónico + comuna)

Joins espaciales: usamos la geometría como string GeoJSON parsed con un
Python UDF (no necesitamos Sedona para Sprint 1; hacemos point-in-polygon
con el algoritmo ray-casting clásico).

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/silver/transform_all.py
"""

from __future__ import annotations

import json
import sys
from typing import Optional

sys.path.insert(0, "/workspace/src")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared.bronze_utils import log_err, log_ok, log_seccion, log_warn
from shared.config import (
    SIATA_NULL_SENTINEL,
    TBL_BRONZE_ENCICLA,
    TBL_BRONZE_GEOMEDELLIN,
    TBL_BRONZE_MEDATA,
    TBL_BRONZE_METRO,
    TBL_BRONZE_SIATA,
    TBL_BRONZE_SIMM,
    TBL_SILVER_AFLUENCIA,
    TBL_SILVER_AFOROS,
    TBL_SILVER_AIRE,
    TBL_SILVER_ENCICLA,
    TBL_SILVER_INCIDENTES,
    crear_spark_session,
)


# ── Utilidades geo ──────────────────────────────────────────────────────────


def _punto_en_anillo(lon: float, lat: float, anillo: list) -> bool:
    """Ray-casting clásico para point-in-polygon."""
    n = len(anillo)
    dentro = False
    j = n - 1
    for i in range(n):
        xi, yi = anillo[i][0], anillo[i][1]
        xj, yj = anillo[j][0], anillo[j][1]
        if ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / (yj - yi + 1e-12) + xi
        ):
            dentro = not dentro
        j = i
    return dentro


def _construir_indice_comunas(filas_geo) -> list[tuple[str, str, list]]:
    """
    Devuelve [(nombre, tipo, anillo_exterior), ...] para usar en el UDF.
    El anillo se serializa como lista de [lon, lat].
    """
    indice = []
    for r in filas_geo:
        try:
            geom = json.loads(r["geometry_geojson"])
        except Exception:
            continue
        if not geom:
            continue
        coords = geom.get("coordinates", [])
        if not coords:
            continue
        anillo = coords[0]  # exterior; ignoramos huecos
        indice.append((r["nombre"], r["tipo"], anillo))
    return indice


def _udf_asignar_comuna(indice):
    """Construye una UDF que dado (lon, lat) devuelve nombre de comuna o None."""
    indice_local = indice  # se serializa con el closure

    def asignar(lon, lat):
        if lon is None or lat is None:
            return None
        try:
            lonf, latf = float(lon), float(lat)
        except (TypeError, ValueError):
            return None
        for nombre, _tipo, anillo in indice_local:
            if _punto_en_anillo(lonf, latf, anillo):
                return nombre
        return None

    return F.udf(asignar, StringType())


# ── Silver builders ─────────────────────────────────────────────────────────


def _silver_incidentes(spark: SparkSession, asignar_comuna) -> int:
    log_seccion("Silver · incidentes_geocodificados (MEData)")

    bronze = spark.table(TBL_BRONZE_MEDATA)

    # Coordenadas: MEData trae LOCATION = "[lon, lat]" como string.
    # Pre-2017 hay un % de filas con lat/lon invertidos. Heurística:
    # Medellín está en lon ≈ -75.6, lat ≈ 6.2. Si la primera coord cae en
    # rango de latitud (5..7) y la segunda en rango de longitud (-76..-75),
    # están invertidas.
    df = bronze.withColumn(
        "loc_clean",
        F.regexp_replace(F.col("location"), r"[\[\]\s]", ""),
    ).withColumn(
        "_a", F.split(F.col("loc_clean"), ",").getItem(0).cast("double")
    ).withColumn(
        "_b", F.split(F.col("loc_clean"), ",").getItem(1).cast("double")
    )

    # ¿Está invertido? (a en rango lat, b en rango lon)
    invertido = (F.col("_a").between(4.5, 7.5)) & (F.col("_b").between(-77.0, -74.5))
    df = df.withColumn(
        "longitud",
        F.when(invertido, F.col("_b")).otherwise(F.col("_a")),
    ).withColumn(
        "latitud",
        F.when(invertido, F.col("_a")).otherwise(F.col("_b")),
    )

    # Filtrar fuera del bounding box del Valle de Aburrá (limpia ~3% basura)
    df = df.filter(
        F.col("longitud").between(-76.0, -75.0)
        & F.col("latitud").between(5.7, 6.7)
    )

    # Casting de fechas: FECHA_ACCIDENTES viene en ISO8601
    df = df.withColumn("fecha_accidente_ts", F.to_timestamp("fecha_accidentes"))
    df = df.withColumn("anio_accidente", F.year("fecha_accidente_ts"))
    df = df.withColumn("mes_accidente", F.month("fecha_accidente_ts"))

    # Asignar comuna por geometría OSM (sobreescribe la columna textual de MEData
    # cuando el join espacial encuentra una; si no, deja el valor original).
    df = df.withColumn("comuna_geo", asignar_comuna(F.col("longitud"), F.col("latitud")))
    df = df.withColumn(
        "comuna_final",
        F.coalesce(F.col("comuna_geo"), F.col("comuna")),
    )

    # Deduplicación por número de radicado
    df = df.dropDuplicates(["nro_radicado"])

    out = df.select(
        F.col("nro_radicado").alias("incidente_id"),
        "fecha_accidente_ts",
        "anio_accidente",
        "mes_accidente",
        F.col("clase_accidente").alias("clase"),
        F.col("gravedad_accidente").alias("gravedad"),
        F.col("diseno").alias("diseno_via"),
        "longitud",
        "latitud",
        F.col("barrio").alias("barrio_medata"),
        F.col("comuna_final").alias("comuna"),
        "direccion",
    )

    n = out.count()
    out.writeTo(TBL_SILVER_INCIDENTES).using("iceberg").partitionedBy(F.col("anio_accidente")).createOrReplace()
    log_ok(f"{TBL_SILVER_INCIDENTES}: {n:,} filas (deduplicadas y geocodificadas)")
    return n


def _silver_aire(spark: SparkSession) -> int:
    log_seccion("Silver · lecturas_aire_validas (SIATA)")

    bronze = spark.table(TBL_BRONZE_SIATA)
    sentinel = SIATA_NULL_SENTINEL

    df = bronze
    for col in ("pm25", "pm10", "temperatura_c", "humedad_pct", "precipitacion_mm", "viento_kmh"):
        df = df.withColumn(
            col,
            F.when(F.col(col) == F.lit(sentinel), None).otherwise(F.col(col)),
        )

    # Filtrar lecturas sin PM2.5 (la métrica primaria)
    df = df.filter(F.col("pm25").isNotNull() & (F.col("pm25") > 0))

    df = df.withColumn("anio", F.year("timestamp"))
    df = df.withColumn("hora", F.hour("timestamp"))
    df = df.withColumn(
        "fecha_hora",
        F.date_trunc("hour", F.col("timestamp")),
    )

    out = df.select(
        "estacion_id", "estacion_nombre", "zona", "latitud", "longitud",
        "timestamp", "fecha_hora", "anio", "hora",
        "pm25", "pm10", "temperatura_c", "humedad_pct",
        "precipitacion_mm", "viento_kmh",
    )

    n = out.count()
    out.writeTo(TBL_SILVER_AIRE).using("iceberg").partitionedBy(F.col("anio")).createOrReplace()
    log_ok(f"{TBL_SILVER_AIRE}: {n:,} filas válidas")
    return n


def _silver_afluencia(spark: SparkSession) -> int:
    log_seccion("Silver · afluencia_horaria (Metro + agregado SIATA diario)")

    metro = spark.table(TBL_BRONZE_METRO).select(
        "fecha", "estacion_id", "estacion_nombre", "linea", "validaciones",
    )

    # Agregar lluvia y PM2.5 diario por zona promedio (toda la red)
    aire_dia = (
        spark.table(TBL_SILVER_AIRE)
        .withColumn("fecha", F.to_date("timestamp"))
        .groupBy("fecha")
        .agg(
            F.avg("pm25").alias("pm25_promedio_red"),
            F.sum("precipitacion_mm").alias("precipitacion_total_mm"),
            F.avg("temperatura_c").alias("temperatura_promedio_c"),
        )
    )

    out = (
        metro.join(aire_dia, on="fecha", how="left")
        .withColumn("anio", F.year("fecha"))
        .withColumn("dia_semana", F.dayofweek("fecha"))
        .withColumn("es_finde", (F.col("dia_semana").isin(1, 7)).cast("int"))
    )

    n = out.count()
    out.writeTo(TBL_SILVER_AFLUENCIA).using("iceberg").partitionedBy(F.col("anio")).createOrReplace()
    log_ok(f"{TBL_SILVER_AFLUENCIA}: {n:,} filas (Metro × clima diario)")
    return n


def _silver_encicla(spark: SparkSession) -> int:
    log_seccion("Silver · viajes_encicla_anonimizados")

    bronze = spark.table(TBL_BRONZE_ENCICLA)

    df = (
        bronze.filter(F.col("ts_inicio").isNotNull())
        .withColumn("fecha", F.to_date("ts_inicio"))
        .withColumn("hora", F.hour("ts_inicio"))
        .withColumn("anio", F.year("ts_inicio"))
        .withColumn(
            "categoria_duracion",
            F.when(F.col("duracion_min") < 10, "corta")
            .when(F.col("duracion_min") < 30, "media")
            .otherwise("larga"),
        )
        .filter(F.col("duracion_min").between(1, 240))
        .dropDuplicates(["id_viaje"])
    )

    # Cruza con clima diario
    aire_dia = (
        spark.table(TBL_SILVER_AIRE)
        .withColumn("fecha", F.to_date("timestamp"))
        .groupBy("fecha")
        .agg(
            F.avg("temperatura_c").alias("temperatura_promedio_c"),
            F.avg("pm25").alias("pm25_promedio"),
            F.sum("precipitacion_mm").alias("precipitacion_total_mm"),
        )
    )

    out = df.join(aire_dia, on="fecha", how="left").select(
        "id_viaje", "usuario_pseudo", "estacion_origen", "estacion_destino",
        "ts_inicio", "ts_fin", "duracion_min", "categoria_duracion",
        "fecha", "hora", "anio",
        "temperatura_promedio_c", "pm25_promedio", "precipitacion_total_mm",
    )

    n = out.count()
    out.writeTo(TBL_SILVER_ENCICLA).using("iceberg").partitionedBy(F.col("anio")).createOrReplace()
    log_ok(f"{TBL_SILVER_ENCICLA}: {n:,} viajes")
    return n


def _silver_aforos(spark: SparkSession, asignar_comuna) -> int:
    log_seccion("Silver · aforos_corredor_geo (SIMM)")

    bronze = spark.table(TBL_BRONZE_SIMM)

    # SIMM Bronze unió aforos manuales + cámaras. Cada uno tiene cols diferentes.
    # Estrategia: normalizar a un esquema mínimo {fecha, longitud, latitud,
    # corredor, vehiculos_aprox, tipo_aforo}.

    # Coordenadas según tipo
    df = (
        bronze
        # Aforos manuales: COORDENADAX/Y son lon/lat; corredor = via_principal
        .withColumn(
            "longitud",
            F.coalesce(
                F.col("coordenadax").cast("double"),
                # Para cámaras: location viene como dict-string {'lon':..,'lat':..}
                F.regexp_extract(F.col("location"), r"'lon'\s*:\s*'([^']+)'", 1).cast("double"),
            ),
        )
        .withColumn(
            "latitud",
            F.coalesce(
                F.col("coordenaday").cast("double"),
                F.regexp_extract(F.col("location"), r"'lat'\s*:\s*'([^']+)'", 1).cast("double"),
            ),
        )
        .withColumn(
            "corredor",
            F.coalesce(F.col("via_principal"), F.col("corredor")),
        )
        .withColumn(
            "fecha",
            F.coalesce(
                F.to_date(F.col("fecha_hora").substr(1, 10), "yyyy-MM-dd"),
                F.to_date(F.col("fechahora").substr(1, 10), "yyyy-MM-dd"),
            ),
        )
        .withColumn(
            "vehiculos_aprox",
            F.when(
                F.col("tipo_aforo") == "manual",
                F.coalesce(F.col("autos").cast("int"), F.lit(0))
                + F.coalesce(F.col("buses").cast("int"), F.lit(0))
                + F.coalesce(F.col("camiones").cast("int"), F.lit(0))
                + F.coalesce(F.col("motos").cast("int"), F.lit(0)),
            ).otherwise(F.col("intensidad").cast("int")),
        )
        .filter(
            F.col("longitud").between(-76.0, -75.0)
            & F.col("latitud").between(5.7, 6.7)
        )
    )

    df = df.withColumn("comuna", asignar_comuna(F.col("longitud"), F.col("latitud")))
    df = df.withColumn("anio", F.year("fecha"))

    out = df.select(
        "tipo_aforo", "corredor", "comuna",
        "longitud", "latitud", "fecha", "anio",
        F.col("vehiculos_aprox").alias("vehiculos"),
    ).filter(F.col("vehiculos").isNotNull())

    n = out.count()
    out.writeTo(TBL_SILVER_AFOROS).using("iceberg").partitionedBy(F.col("anio")).createOrReplace()
    log_ok(f"{TBL_SILVER_AFOROS}: {n:,} filas (corredores geocodificados)")
    return n


def main() -> int:
    spark = crear_spark_session("Silver-All")
    spark.sparkContext.setLogLevel("WARN")

    # 1. Cargar índice de comunas a memoria del driver para los UDF spaciales
    log_seccion("Construyendo índice de comunas (driver)")
    geo_filas = spark.table(TBL_BRONZE_GEOMEDELLIN).select("nombre", "tipo", "geometry_geojson").collect()
    indice = _construir_indice_comunas(geo_filas)
    log_ok(f"Índice listo: {len(indice)} polígonos cargados")
    asignar_comuna = _udf_asignar_comuna(indice)

    # 2. Construir cada Silver
    _silver_aire(spark)               # SIATA primero (depende solo de Bronze)
    _silver_afluencia(spark)          # Metro + Silver SIATA
    _silver_incidentes(spark, asignar_comuna)
    _silver_encicla(spark)
    _silver_aforos(spark, asignar_comuna)

    log_seccion("✅ Silver completo")
    return 0


if __name__ == "__main__":
    sys.exit(main())
