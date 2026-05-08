"""
bronze_utils.py — Utilidades comunes para los scripts de ingesta Bronze.

Todas las ingestas Bronze de Pulso Medellín comparten 4 cosas:
    1. Insertan columnas de auditoría (timestamp_ingesta, nombre_archivo, fuente_id, fecha_ingesta).
    2. Hacen append, particionado por fecha_ingesta.
    3. Toleran archivos faltantes con un warning (no rompen el pipeline).
    4. Usan el mismo logger / formato de salida.

Importar desde los scripts de bronze:
    from shared.bronze_utils import (
        agregar_columnas_auditoria, escribir_bronze, log_seccion, log_ok, log_warn, log_err,
    )
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.config import (
    COL_FECHA_INGESTA,
    COL_FUENTE_ID,
    COL_NOMBRE_ARCHIVO,
    COL_TIMESTAMP_INGESTA,
)

# ── Logging plano (no usar logging.* para que se vea bien en docker logs) ────

def log_seccion(msg: str) -> None:
    print(f"\n━━━ {msg} ━━━", flush=True)


def log_ok(msg: str) -> None:
    print(f"  ✓ {msg}", flush=True)


def log_warn(msg: str) -> None:
    print(f"  ⚠ {msg}", flush=True)


def log_err(msg: str) -> None:
    print(f"  ✗ {msg}", flush=True)


# ── Auditoría + escritura Bronze ─────────────────────────────────────────────


def agregar_columnas_auditoria(df: DataFrame, fuente_id: str, nombre_archivo: str) -> DataFrame:
    """
    Inyecta las 4 columnas de auditoría que TODA tabla Bronze debe llevar.
    Se aplican como `lit()` para minimizar overhead.
    """
    ahora = datetime.utcnow().isoformat(timespec="seconds")
    fecha = ahora[:10]
    return (
        df.withColumn(COL_TIMESTAMP_INGESTA, F.lit(ahora).cast("timestamp"))
          .withColumn(COL_NOMBRE_ARCHIVO, F.lit(nombre_archivo))
          .withColumn(COL_FUENTE_ID, F.lit(fuente_id))
          .withColumn(COL_FECHA_INGESTA, F.lit(fecha).cast("date"))
    )


def escribir_bronze(
    spark: SparkSession,
    df: DataFrame,
    tabla: str,
    fuente_id: str,
    nombre_archivo: str,
    particion: str = COL_FECHA_INGESTA,
) -> int:
    """
    Append idempotente sobre la tabla Bronze. Devuelve filas insertadas.

    - Si la tabla no existe, la crea con `CREATE TABLE ... USING iceberg`.
    - Si existe, hace append. La partición por defecto es `fecha_ingesta`.
    """
    df_audit = agregar_columnas_auditoria(df, fuente_id, nombre_archivo)
    n = df_audit.count()
    if n == 0:
        log_warn(f"{tabla}: 0 filas — nada que insertar")
        return 0

    existe = spark.catalog.tableExists(tabla)
    if not existe:
        log_seccion(f"Creando tabla Iceberg {tabla}")
        (
            df_audit.writeTo(tabla)
            .using("iceberg")
            .partitionedBy(F.col(particion))
            .createOrReplace()
        )
    else:
        log_seccion(f"Append a tabla existente {tabla}")
        df_audit.writeTo(tabla).append()

    log_ok(f"{tabla}: {n:,} filas insertadas")
    return n


# ── Manejo defensivo de archivos faltantes ──────────────────────────────────


def archivos_existentes(rutas: Iterable[str]) -> list[str]:
    """
    Filtra rutas que existen y tienen tamaño > 0. Loguea las que faltan.
    Útil cuando una fuente puede traer 1, 2 o N archivos según lo que se descargó.
    """
    encontrados = []
    for ruta in rutas:
        p = Path(ruta)
        if p.exists() and p.stat().st_size > 0:
            encontrados.append(str(p))
        else:
            log_warn(f"No encontrado o vacío: {ruta}")
    return encontrados


def setup_path_proyecto() -> None:
    """
    Inserta /workspace/src en sys.path para que `from shared.config import ...`
    funcione cuando el script se ejecuta dentro del contenedor.
    """
    base = "/workspace/src"
    if base not in sys.path:
        sys.path.insert(0, base)
