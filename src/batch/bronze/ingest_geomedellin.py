"""
ingest_geomedellin.py — Bronze · GeoMedellín comunas y corregimientos.

Fuente: data/raw/geomedellin/comunas_corregimientos.geojson  (generado por
scripts/overpass_a_geojson.py desde OpenStreetMap).

Modelo Bronze: una fila por polígono (comuna o corregimiento) con:
    - osm_id
    - nombre, tipo (comuna|corregimiento), admin_level
    - geometry_geojson (string JSON del polígono — Spark no tiene tipo geo nativo)

Esta tabla es una "dimensión" geográfica usada en Silver para los joins
espaciales (incidentes ↔ comuna, estaciones EnCicla ↔ barrio, etc.).

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_geomedellin.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import Row

from shared.bronze_utils import (
    escribir_bronze,
    log_err,
    log_ok,
    log_seccion,
    log_warn,
)
from shared.config import TBL_BRONZE_GEOMEDELLIN, crear_spark_session

GEOJSON = "/workspace/data/raw/geomedellin/comunas_corregimientos.geojson"
FUENTE_ID = "geomedellin_osm"


def main() -> int:
    log_seccion("Bronze · GeoMedellín (comunas/corregimientos)")

    p = Path(GEOJSON)
    if not p.exists() or p.stat().st_size == 0:
        log_err(f"No encontrado: {GEOJSON}")
        log_warn("Ejecutar primero: bash scripts/download_datasets.sh")
        return 1

    with open(GEOJSON, encoding="utf-8") as f:
        gj = json.load(f)

    features = gj.get("features", [])
    log_ok(f"GeoJSON parseado: {len(features)} features")

    filas = []
    for feat in features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry", {}) or {}
        filas.append(
            Row(
                osm_id=int(props.get("osm_id", 0) or 0),
                nombre=props.get("nombre", "") or "",
                tipo=props.get("tipo", "") or "",
                admin_level=str(props.get("admin_level", "") or ""),
                nombre_alt=props.get("nombre_alt", "") or "",
                wikipedia=props.get("wikipedia", "") or "",
                geometry_geojson=json.dumps(geom, ensure_ascii=False),
            )
        )

    if not filas:
        log_err("0 features — nada que ingestar")
        return 2

    spark = crear_spark_session("Bronze-GeoMedellin")
    spark.sparkContext.setLogLevel("WARN")
    df = spark.createDataFrame(filas)

    n = escribir_bronze(
        spark=spark,
        df=df,
        tabla=TBL_BRONZE_GEOMEDELLIN,
        fuente_id=FUENTE_ID,
        nombre_archivo=p.name,
    )
    log_ok(f"Bronze GeoMedellín listo: {n} polígonos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
