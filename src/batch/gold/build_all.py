"""
build_all.py — Silver → Gold para responder las 4 preguntas analíticas B-1..B-4.

Las 4 tablas Gold son agregaciones de negocio listas para ser consumidas
por notebooks, dashboards o Trino.

    B-1  afluencia_vs_pm25         · ¿correlación lluvia/aire–afluencia Metro?
    B-2  accidentalidad_por_comuna · ¿cuáles comunas concentran severidad?
    B-3  demanda_encicla_vs_clima  · ¿bajo qué umbrales cae demanda EnCicla?
    B-4  corredores_riesgo_compuesto · ¿qué corredores combinan volumen + accidentes?

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/gold/build_all.py
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/src")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from shared.bronze_utils import log_ok, log_seccion
from shared.config import (
    TBL_GOLD_ACCIDENTALIDAD,
    TBL_GOLD_AFLUENCIA_PM25,
    TBL_GOLD_CORREDORES_RIESGO,
    TBL_GOLD_ENCICLA_CLIMA,
    TBL_GOLD_PERCENTILES_METRO,
    TBL_SILVER_AFLUENCIA,
    TBL_SILVER_AFOROS,
    TBL_SILVER_ENCICLA,
    TBL_SILVER_INCIDENTES,
    crear_spark_session,
)


# ── B-1 · afluencia vs PM2.5 / lluvia ───────────────────────────────────────


def b1(spark: SparkSession) -> int:
    log_seccion("Gold B-1 · afluencia_vs_pm25")
    af = spark.table(TBL_SILVER_AFLUENCIA)

    # Granularidad: línea × mes (la fuente real Metro NO desglosa por estación).
    # Métricas: pasajeros totales y promedio diario, PM2.5 promedio de la red,
    # precipitación total mensual, y correlaciones intra-mes (Pearson) entre
    # contaminación/lluvia y afluencia diaria.
    diario = (
        af.groupBy(F.col("fecha"), F.col("linea"))
          .agg(
              F.sum("pasajeros").alias("pasajeros_dia"),
              F.avg("pm25_promedio_red").alias("pm25_dia"),
              F.first("precipitacion_total_mm", ignorenulls=True).alias("precip_dia"),
          )
    )

    mensual = (
        diario.groupBy(
            F.year("fecha").alias("anio"),
            F.month("fecha").alias("mes"),
            "linea",
        ).agg(
            F.sum("pasajeros_dia").alias("pasajeros_mes"),
            F.avg("pasajeros_dia").alias("pasajeros_promedio_dia"),
            F.avg("pm25_dia").alias("pm25_promedio_mes"),
            F.sum("precip_dia").alias("precipitacion_total_mes_mm"),
            F.corr("pm25_dia", "pasajeros_dia").alias("corr_pm25_pasajeros"),
            F.corr("precip_dia", "pasajeros_dia").alias("corr_precip_pasajeros"),
            F.count("*").alias("dias_observados"),
        )
    )

    n = mensual.count()
    mensual.writeTo(TBL_GOLD_AFLUENCIA_PM25).using("iceberg").partitionedBy("anio").createOrReplace()
    log_ok(f"{TBL_GOLD_AFLUENCIA_PM25}: {n:,} filas (línea × mes)")
    return n


# ── B-2 · accidentalidad por comuna ─────────────────────────────────────────


def b2(spark: SparkSession) -> int:
    log_seccion("Gold B-2 · accidentalidad_por_comuna")
    inc = spark.table(TBL_SILVER_INCIDENTES)

    # Conteo por comuna × año × gravedad
    base = (
        inc.filter(F.col("comuna").isNotNull())
        .groupBy(
            F.col("anio_accidente").alias("anio"),
            "comuna",
            "gravedad",
        ).agg(
            F.count("*").alias("incidentes"),
        )
    )

    # Pivot a wide (con_heridos, con_muertos, solo_danos) + total
    pivot = (
        base.groupBy("anio", "comuna")
        .pivot("gravedad")
        .agg(F.sum("incidentes"))
        .na.fill(0)
    )

    # Renombrar columnas del pivot a snake_case sin tildes/espacios
    rename = {
        "Con heridos": "con_heridos",
        "Con muertos": "con_muertos",
        "Solo daños": "solo_danos",
    }
    for orig, nuevo in rename.items():
        if orig in pivot.columns:
            pivot = pivot.withColumnRenamed(orig, nuevo)

    # Asegurar que las 3 columnas existan aunque algún año no tenga muertos
    for c in ("con_heridos", "con_muertos", "solo_danos"):
        if c not in pivot.columns:
            pivot = pivot.withColumn(c, F.lit(0))

    pivot = pivot.withColumn(
        "incidentes_total",
        F.col("con_heridos") + F.col("con_muertos") + F.col("solo_danos"),
    ).withColumn(
        "indice_severidad",
        # peso: 0.1 daños + 1.0 heridos + 5.0 muertos (estándar OMS-like)
        (
            F.col("solo_danos") * 0.1
            + F.col("con_heridos") * 1.0
            + F.col("con_muertos") * 5.0
        ),
    )

    # Ranking dentro de cada año
    w = Window.partitionBy("anio").orderBy(F.desc("indice_severidad"))
    out = pivot.withColumn("ranking_severidad", F.row_number().over(w))

    n = out.count()
    out.writeTo(TBL_GOLD_ACCIDENTALIDAD).using("iceberg").partitionedBy("anio").createOrReplace()
    log_ok(f"{TBL_GOLD_ACCIDENTALIDAD}: {n:,} filas (comuna × año)")
    return n


# ── B-3 · demanda EnCicla vs clima ──────────────────────────────────────────


def b3(spark: SparkSession) -> int:
    log_seccion("Gold B-3 · demanda_encicla_vs_clima")
    enc = spark.table(TBL_SILVER_ENCICLA)

    # Bin temperatura en 4°C; Bin PM2.5 cada 15 µg/m³.
    df = (
        enc.withColumn(
            "bin_temperatura",
            F.when(F.col("temperatura_promedio_c").isNull(), "sin_dato")
            .otherwise(
                F.concat_ws("-",
                    (F.floor(F.col("temperatura_promedio_c") / 4) * 4).cast("string"),
                    ((F.floor(F.col("temperatura_promedio_c") / 4) * 4) + 4).cast("string"),
                )
            ),
        )
        .withColumn(
            "bin_pm25",
            F.when(F.col("pm25_promedio").isNull(), "sin_dato")
            .otherwise(
                F.concat_ws("-",
                    (F.floor(F.col("pm25_promedio") / 15) * 15).cast("string"),
                    ((F.floor(F.col("pm25_promedio") / 15) * 15) + 15).cast("string"),
                )
            ),
        )
        .withColumn(
            "llovio",
            F.when(F.col("precipitacion_total_mm") > 1.0, 1).otherwise(0),
        )
    )

    # Demanda diaria por bin. Para luego responder elasticidad, también la
    # comparamos contra demanda media histórica.
    diaria = df.groupBy("fecha", "bin_temperatura", "bin_pm25", "llovio").agg(
        F.count("*").alias("viajes"),
        F.avg("duracion_min").alias("duracion_promedio_min"),
    )

    media = diaria.agg(F.avg("viajes").alias("viajes_promedio_global")).first()["viajes_promedio_global"]

    out = diaria.withColumn(
        "viajes_relativos_pct",
        (F.col("viajes") / F.lit(media) - 1.0) * 100,
    ).withColumn("anio", F.year("fecha"))

    n = out.count()
    out.writeTo(TBL_GOLD_ENCICLA_CLIMA).using("iceberg").partitionedBy("anio").createOrReplace()
    log_ok(f"{TBL_GOLD_ENCICLA_CLIMA}: {n:,} filas (día × clima)")
    return n


# ── B-4 · corredores de riesgo compuesto ────────────────────────────────────


def b4(spark: SparkSession) -> int:
    log_seccion("Gold B-4 · corredores_riesgo_compuesto")
    af = spark.table(TBL_SILVER_AFOROS)
    inc = spark.table(TBL_SILVER_INCIDENTES)

    # Volumen por comuna (suma vehículos)
    vol_comuna = (
        af.filter(F.col("comuna").isNotNull())
        .groupBy("comuna")
        .agg(
            F.sum("vehiculos").alias("vehiculos_total"),
            F.countDistinct("corredor").alias("corredores_distintos"),
        )
    )

    # Accidentalidad por comuna (todo período)
    inc_comuna = (
        inc.filter(F.col("comuna").isNotNull())
        .groupBy("comuna")
        .agg(
            F.count("*").alias("incidentes_total"),
            F.sum(F.when(F.col("gravedad") == "Con muertos", 1).otherwise(0)).alias("con_muertos"),
            F.sum(F.when(F.col("gravedad") == "Con heridos", 1).otherwise(0)).alias("con_heridos"),
        )
    )

    j = vol_comuna.join(inc_comuna, on="comuna", how="inner")

    # Score compuesto: normalizar volumen y severidad a percentiles, sumar.
    # Más simple: rank por cada métrica.
    w_v = Window.orderBy(F.desc("vehiculos_total"))
    w_s = Window.orderBy(F.desc(F.col("con_muertos") * 5 + F.col("con_heridos")))

    out = (
        j.withColumn("rank_volumen", F.row_number().over(w_v))
         .withColumn("rank_severidad", F.row_number().over(w_s))
         .withColumn("score_riesgo", F.col("rank_volumen") + F.col("rank_severidad"))
    )

    out = out.orderBy("score_riesgo")
    n = out.count()
    out.writeTo(TBL_GOLD_CORREDORES_RIESGO).using("iceberg").createOrReplace()
    log_ok(f"{TBL_GOLD_CORREDORES_RIESGO}: {n:,} filas (comuna)")
    return n


def b5_percentiles_metro(spark: SparkSession) -> int:
    """Gold derivada · percentiles de afluencia por (línea × franja horaria).

    Esta tabla es el insumo del job híbrido del Sprint 3 cuando consume Gold
    en vivo (Sprint 4 — migración desde JSON precomputado a PyIceberg).
    """
    log_seccion("Gold · percentiles_metro (insumo job híbrido)")
    af = spark.table(TBL_SILVER_AFLUENCIA)

    # `franja_horaria` viene precalculada desde Silver
    base = af.filter(F.col("pasajeros") > 0)

    salida = (
        base.groupBy("linea", "franja_horaria")
        .agg(
            F.expr("percentile_approx(pasajeros, 0.50)").cast("long").alias("p50"),
            F.expr("percentile_approx(pasajeros, 0.75)").cast("long").alias("p75"),
            F.expr("percentile_approx(pasajeros, 0.90)").cast("long").alias("p90"),
            F.expr("percentile_approx(pasajeros, 0.95)").cast("long").alias("p95"),
            F.count("*").cast("long").alias("muestras"),
        )
        .filter(F.col("muestras") >= 10)
    )

    n = salida.count()
    salida.writeTo(TBL_GOLD_PERCENTILES_METRO).using("iceberg").createOrReplace()
    log_ok(f"{TBL_GOLD_PERCENTILES_METRO}: {n:,} filas (línea × franja_horaria)")
    return n


def main() -> int:
    spark = crear_spark_session("Gold-All")
    spark.sparkContext.setLogLevel("WARN")
    b1(spark)
    b2(spark)
    b3(spark)
    b4(spark)
    b5_percentiles_metro(spark)
    log_seccion("✅ Gold completo (B-1..B-4 + percentiles_metro)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
