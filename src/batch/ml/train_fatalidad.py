"""
train_fatalidad.py — Módulo 06a · MLlib: clasificación multiclase de gravedad en incidentes viales.

Pipeline ML sobre silver.incidentes_geocodificados:
  - Features: hora, día semana, mes, clase_accidente, diseño_vía, comuna, longitud, latitud
  - Target: gravedad (multiclase: Solo daños / Con heridos / Con muertos)
  - Modelo: RandomForestClassifier (100 árboles, profundidad 10)
  - Evaluación: accuracy, F1-weighted, precision-weighted, recall-weighted

Salidas:
  - gold.ml_fatalidad_evaluacion  (métricas de evaluación en conjunto de prueba)
  - /workspace/data/processed/modelos/fatalidad_rf/  (modelo serializado)

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/ml/train_fatalidad.py
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/src")

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.bronze_utils import log_ok, log_seccion, log_warn
from shared.config import (
    TBL_GOLD_ML_FATALIDAD_EVAL,
    TBL_SILVER_INCIDENTES,
    crear_spark_session,
)

SEMILLA = 42
N_ARBOLES = 100
MAX_PROFUNDIDAD = 10
PROP_TEST = 0.2

CLASES_VALIDAS = ("Con heridos", "Con muertos", "Solo daños")
COLS_CATEGORICAS = ["clase", "diseno_via", "comuna"]
COLS_NUMERICAS = ["hora", "dia_semana", "mes_accidente", "longitud", "latitud"]


def _preparar_features(df):
    """Extrae features temporales, filtra clases válidas y limpia nulos."""
    return (
        df.filter(F.col("gravedad").isin(*CLASES_VALIDAS))
          .filter(F.col("fecha_accidente_ts").isNotNull())
          .withColumn("hora", F.hour("fecha_accidente_ts"))
          .withColumn("dia_semana", F.dayofweek("fecha_accidente_ts"))
          .fillna("DESCONOCIDO", subset=COLS_CATEGORICAS)
          .fillna(0.0, subset=["longitud", "latitud"])
    )


def _construir_pipeline() -> Pipeline:
    """Arma el Pipeline MLlib: StringIndexer → OHE → VectorAssembler → RF."""
    label_indexer = StringIndexer(
        inputCol="gravedad", outputCol="label", handleInvalid="keep"
    )
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in COLS_CATEGORICAS
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe")
        for c in COLS_CATEGORICAS
    ]
    feature_cols = [f"{c}_ohe" for c in COLS_CATEGORICAS] + COLS_NUMERICAS
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="keep"
    )
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=N_ARBOLES,
        maxDepth=MAX_PROFUNDIDAD,
        seed=SEMILLA,
    )
    return Pipeline(stages=[label_indexer, *indexers, *encoders, assembler, rf])


def _evaluar(predicciones, spark: SparkSession):
    """Calcula métricas sobre el conjunto de prueba y devuelve un DataFrame."""
    metricas_nombres = [
        ("accuracy",           "Accuracy"),
        ("f1",                 "F1 (weighted)"),
        ("weightedPrecision",  "Precision (weighted)"),
        ("weightedRecall",     "Recall (weighted)"),
    ]

    resultados = {}
    for metrica_key, etiqueta in metricas_nombres:
        evaluador = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName=metrica_key,
        )
        valor = evaluador.evaluate(predicciones)
        resultados[metrica_key] = valor
        log_ok(f"  {etiqueta:25s} {valor:.4f}")

    return spark.createDataFrame([{
        "modelo":               "RandomForestClassifier",
        "n_arboles":            N_ARBOLES,
        "max_profundidad":      MAX_PROFUNDIDAD,
        "prop_test":            PROP_TEST,
        "semilla":              SEMILLA,
        "accuracy":             float(resultados["accuracy"]),
        "f1_weighted":          float(resultados["f1"]),
        "precision_weighted":   float(resultados["weightedPrecision"]),
        "recall_weighted":      float(resultados["weightedRecall"]),
    }])


def main() -> int:
    log_seccion("Módulo 06a · MLlib — Clasificación de gravedad en incidentes viales")

    spark = crear_spark_session("ML-Fatalidad-Sprint5")
    spark.sparkContext.setLogLevel("WARN")

    log_seccion("Cargando silver.incidentes_geocodificados...")
    df_raw = spark.table(TBL_SILVER_INCIDENTES)
    df = _preparar_features(df_raw)

    total = df.count()
    log_ok(f"Registros disponibles para ML: {total:,}")

    if total < 200:
        log_warn("Muy pocos registros. Verifica que la ingesta de MEData esté completa.")
        return 1

    log_seccion("Distribución de la variable objetivo (gravedad):")
    df.groupBy("gravedad").count().orderBy(F.desc("count")).show(truncate=False)

    df_train, df_test = df.randomSplit([1.0 - PROP_TEST, PROP_TEST], seed=SEMILLA)
    log_ok(f"Train: {df_train.count():,} filas | Test: {df_test.count():,} filas")

    log_seccion("Entrenando RandomForestClassifier...")
    pipeline = _construir_pipeline()
    modelo = pipeline.fit(df_train)

    log_seccion("Evaluando en conjunto de prueba:")
    predicciones = modelo.transform(df_test)
    metricas_df = _evaluar(predicciones, spark)

    metricas_df.writeTo(TBL_GOLD_ML_FATALIDAD_EVAL).using("iceberg").createOrReplace()
    log_ok(f"Métricas guardadas en {TBL_GOLD_ML_FATALIDAD_EVAL}")

    ruta_modelo = "/workspace/data/processed/modelos/fatalidad_rf"
    modelo.write().overwrite().save(ruta_modelo)
    log_ok(f"Modelo serializado en {ruta_modelo}")

    rf_stage = modelo.stages[-1]
    log_ok(f"Features en vector: {len(rf_stage.featureImportances)}")
    log_ok(f"Profundidad media de árboles: {sum(t.depth for t in rf_stage.trees) / N_ARBOLES:.1f}")

    log_seccion("✅ Módulo 06a MLlib completado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
