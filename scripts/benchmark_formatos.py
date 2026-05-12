"""
benchmark_formatos.py — Reproduce las mediciones del ADR 04.

Compara tres formas de persistir los datasets más grandes del proyecto:
  · CSV (formato de llegada)
  · Parquet con compresión Snappy (default Spark)
  · Parquet con compresión ZSTD (default Iceberg 1.4+)

Métricas:
  · tamaño en disco (bytes)
  · tiempo de escritura
  · tiempo de lectura full scan
  · tiempo de lectura con predicado (medir partition pruning)

Cada métrica se mide N_CORRIDAS veces y se reporta la mediana, para evitar
ruido del warm-up de la JVM.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/scripts/benchmark_formatos.py
o desde el host:
    make benchmark-formatos
"""

from __future__ import annotations

import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.config import crear_spark_session

N_CORRIDAS = 3
BASE_OUT = Path("/tmp/benchmark_formatos")

DATASETS = [
    {
        "nombre": "medata_incidentes",
        "ruta_csv": "/workspace/data/raw/medata_incidentes/incidentes_viales.csv",
        "predicado": F.col("AÑO") == "2022",
    },
    {
        "nombre": "siata_pm25_horario",
        "ruta_csv": "/workspace/data/raw/siata_historico/siata_pm25_horario.csv",
        "predicado": F.col("estacion_id") == "MED-BEME",
    },
]


def _du_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            total += p.stat().st_size
    return total


def _medir(funcion, n: int = N_CORRIDAS) -> float:
    tiempos = []
    for _ in range(n):
        t0 = time.perf_counter()
        funcion()
        tiempos.append(time.perf_counter() - t0)
    return statistics.median(tiempos)


def _escribir_csv(df: DataFrame, path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    df.write.option("header", "true").csv(str(path))


def _escribir_parquet(df: DataFrame, path: Path, codec: str) -> None:
    if path.exists():
        shutil.rmtree(path)
    df.write.option("compression", codec).parquet(str(path))


def _leer_count(spark: SparkSession, formato: str, path: Path) -> int:
    if formato == "csv":
        return spark.read.option("header", "true").csv(str(path)).count()
    return spark.read.parquet(str(path)).count()


def _leer_predicado(spark: SparkSession, formato: str, path: Path, predicado) -> int:
    if formato == "csv":
        return spark.read.option("header", "true").csv(str(path)).filter(predicado).count()
    return spark.read.parquet(str(path)).filter(predicado).count()


def benchmark_dataset(spark: SparkSession, dataset: dict) -> dict:
    nombre = dataset["nombre"]
    print(f"\n━━━ {nombre} ━━━", flush=True)

    df = (
        spark.read
        .option("header", "true")
        .csv(dataset["ruta_csv"])
        .cache()
    )
    n_filas = df.count()
    print(f"  filas: {n_filas:,}", flush=True)

    out_base = BASE_OUT / nombre
    out_base.mkdir(parents=True, exist_ok=True)

    resultados = {}
    for formato, escribir_fn in [
        ("csv", lambda p: _escribir_csv(df, p)),
        ("parquet_snappy", lambda p: _escribir_parquet(df, p, "snappy")),
        ("parquet_zstd", lambda p: _escribir_parquet(df, p, "zstd")),
    ]:
        path = out_base / formato
        print(f"  → {formato}", flush=True)

        t_escritura = _medir(lambda: escribir_fn(path))
        tamano = _du_bytes(path)
        t_lectura_full = _medir(lambda: _leer_count(spark, formato.split("_")[0], path))
        t_lectura_pred = _medir(
            lambda: _leer_predicado(spark, formato.split("_")[0], path, dataset["predicado"])
        )

        resultados[formato] = {
            "tamano_bytes": tamano,
            "tamano_mb": round(tamano / (1024 * 1024), 1),
            "t_escritura_s": round(t_escritura, 2),
            "t_lectura_full_s": round(t_lectura_full, 2),
            "t_lectura_predicado_s": round(t_lectura_pred, 2),
        }
        print(
            f"    tamaño={resultados[formato]['tamano_mb']:>6.1f} MB  "
            f"escr={t_escritura:5.2f}s  full={t_lectura_full:5.2f}s  "
            f"pred={t_lectura_pred:5.2f}s",
            flush=True,
        )

    df.unpersist()
    return {"dataset": nombre, "filas": n_filas, "resultados": resultados}


def _imprimir_tabla(resumen: list[dict]) -> None:
    print("\n\n┌─ Resumen ───────────────────────────────────────────────────────────┐")
    for r in resumen:
        print(f"│ {r['dataset']}  ({r['filas']:,} filas)")
        print("│   formato            tamaño     escritura  full-scan  c/predicado")
        for formato, m in r["resultados"].items():
            print(
                f"│   {formato:<18} {m['tamano_mb']:>6.1f} MB    "
                f"{m['t_escritura_s']:>5.2f}s     {m['t_lectura_full_s']:>5.2f}s     "
                f"{m['t_lectura_predicado_s']:>5.2f}s"
            )
        print("│")
    print("└─────────────────────────────────────────────────────────────────────┘")


def main() -> int:
    spark = crear_spark_session("Benchmark-Formatos")
    spark.sparkContext.setLogLevel("ERROR")

    BASE_OUT.mkdir(parents=True, exist_ok=True)
    resumen = []
    for ds in DATASETS:
        if not Path(ds["ruta_csv"]).exists():
            print(f"⚠ saltando {ds['nombre']}: falta {ds['ruta_csv']}")
            continue
        resumen.append(benchmark_dataset(spark, ds))
    _imprimir_tabla(resumen)
    print("\n→ Resultados crudos para actualizar docs/decisiones/04-benchmark-formatos.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
