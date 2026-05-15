"""
demo_iceberg_features.py — Demostración reproducible de los 3 features Iceberg
exigidos por la rúbrica § 4.6.4 (ACID, Time Travel, Schema Evolution) y la
restricción § 3.1 ("Mínimo 2 lotes de ingestión en Bronze").

Crea una tabla `demo.pulsomed.bronze._features_demo` y ejecuta el siguiente
escenario verificable end-to-end:

  1. ACID — escribe el lote 1 (3 filas) y verifica que se observa de forma
     atómica en una nueva snapshot.
  2. Lote 2 — append con metadatos de auditoría → 2 snapshots distintos.
  3. Time Travel — relee la tabla con `VERSION AS OF snap1` y demuestra que
     ve sólo las 3 filas iniciales, no las 6.
  4. Schema Evolution — `ALTER TABLE ADD COLUMN observacion STRING`. Las
     filas viejas devuelven NULL para esa columna; el lote 3 la usa.
  5. Limpieza — DROP TABLE para no contaminar el catálogo.

Salida: imprime cada snapshot con su `snapshot_id`, `committed_at` y
`operation`. El script retorna 0 si los 3 features pasan.

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/scripts/demo_iceberg_features.py
    # o vía Make:  make iceberg-features
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/src")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.config import CATALOG, NS_BRONZE, crear_spark_session

TABLA = f"{CATALOG}.{NS_BRONZE}._features_demo"


def _log(msg: str) -> None:
    print(f"\n━━━ {msg} ━━━", flush=True)


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}", flush=True)


def _err(msg: str) -> None:
    print(f"  ✗ {msg}", flush=True)


def _snapshot_id(spark: SparkSession) -> int:
    """Devuelve el snapshot_id más reciente de la tabla."""
    return (
        spark.sql(f"SELECT snapshot_id FROM {TABLA}.snapshots ORDER BY committed_at DESC LIMIT 1")
        .first()["snapshot_id"]
    )


def _mostrar_snapshots(spark: SparkSession) -> int:
    df = spark.sql(
        f"""
        SELECT snapshot_id,
               committed_at,
               operation,
               summary['added-records'] AS added_records,
               summary['total-records'] AS total_records
        FROM {TABLA}.snapshots
        ORDER BY committed_at
        """
    )
    df.show(truncate=False)
    return df.count()


def main() -> int:
    spark = crear_spark_session("Pulsomed-Iceberg-Features-Demo")
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"DROP TABLE IF EXISTS {TABLA} PURGE")

    # ── Lote 1 — Crear tabla y primera escritura (ACID) ───────────────────────
    _log("§ 4.6.4-ACID · Lote 1 — escritura atómica (3 filas)")
    spark.sql(
        f"""
        CREATE TABLE {TABLA} (
            id BIGINT,
            estacion STRING,
            pm25 DOUBLE,
            timestamp_ingesta TIMESTAMP,
            nombre_archivo STRING,
            fuente_id STRING
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        INSERT INTO {TABLA} VALUES
            (1, 'Poblado',    42.5, current_timestamp(), 'lote_1.csv', 'siata_demo'),
            (2, 'Aranjuez',   58.0, current_timestamp(), 'lote_1.csv', 'siata_demo'),
            (3, 'Itaguí',     71.3, current_timestamp(), 'lote_1.csv', 'siata_demo')
        """
    )
    snap1 = _snapshot_id(spark)
    n1 = spark.table(TABLA).count()
    _ok(f"snapshot 1 = {snap1}  (filas={n1})")
    if n1 != 3:
        _err(f"Se esperaban 3 filas, hay {n1}")
        return 1

    # ── Lote 2 — Append acumulativo (Bronze § 6.1: 2 lotes) ───────────────────
    _log("§ 3.1 (Lotes batch) · Lote 2 — append acumulativo (3 filas más)")
    spark.sql(
        f"""
        INSERT INTO {TABLA} VALUES
            (4, 'Caribe',     65.1, current_timestamp(), 'lote_2.csv', 'siata_demo'),
            (5, 'Manrique',   88.4, current_timestamp(), 'lote_2.csv', 'siata_demo'),
            (6, 'Belén',      54.2, current_timestamp(), 'lote_2.csv', 'siata_demo')
        """
    )
    snap2 = _snapshot_id(spark)
    n2 = spark.table(TABLA).count()
    _ok(f"snapshot 2 = {snap2}  (filas acumuladas={n2})")
    if n2 != 6 or snap1 == snap2:
        _err("Append no produjo nueva snapshot o no acumuló filas")
        return 2

    # ── Time Travel — leer la tabla "como estaba" en el snapshot 1 ─────────────
    _log("§ 4.6.4-Time-Travel · Relectura con VERSION AS OF snap1")
    df_pasado = spark.read.option("snapshot-id", str(snap1)).table(TABLA)
    n_pasado = df_pasado.count()
    df_pasado.orderBy("id").show(truncate=False)
    if n_pasado != 3:
        _err(f"Time Travel falló: se esperaban 3 filas en snap1, hay {n_pasado}")
        return 3
    _ok(f"Time Travel verificado: snap1 tiene {n_pasado} filas, snap2 tiene {n2}")

    # Histórico completo de snapshots
    _log("Histórico de snapshots:")
    n_snaps = _mostrar_snapshots(spark)
    _ok(f"{n_snaps} snapshots registrados")

    # ── Schema Evolution — añadir columna sin reescribir datos ────────────────
    _log("§ 4.6.4-Schema-Evolution · ALTER TABLE ADD COLUMN observacion STRING")
    spark.sql(f"ALTER TABLE {TABLA} ADD COLUMN observacion STRING")

    # Lote 3 con la nueva columna
    spark.sql(
        f"""
        INSERT INTO {TABLA} (id, estacion, pm25, timestamp_ingesta, nombre_archivo, fuente_id, observacion)
        VALUES
            (7, 'Robledo',    49.0, current_timestamp(), 'lote_3.csv', 'siata_demo', 'evento_demo_evolution'),
            (8, 'Castilla',   77.6, current_timestamp(), 'lote_3.csv', 'siata_demo', 'evento_demo_evolution')
        """
    )
    df_post = spark.table(TABLA).select("id", "estacion", "observacion").orderBy("id")
    df_post.show(truncate=False)

    nulos_viejos = df_post.filter(F.col("id") <= 6).filter(F.col("observacion").isNull()).count()
    no_nulos_nuevos = df_post.filter(F.col("id") > 6).filter(F.col("observacion").isNotNull()).count()

    if nulos_viejos != 6:
        _err(f"Esperaba 6 filas viejas con observacion=NULL, hay {nulos_viejos}")
        return 4
    if no_nulos_nuevos != 2:
        _err(f"Esperaba 2 filas nuevas con observacion no nula, hay {no_nulos_nuevos}")
        return 5

    _ok(f"Schema Evolution verificado: {nulos_viejos} filas viejas con NULL, "
        f"{no_nulos_nuevos} filas nuevas con la columna nueva poblada")

    # ── Limpieza ──────────────────────────────────────────────────────────────
    _log("Limpieza")
    spark.sql(f"DROP TABLE {TABLA} PURGE")
    _ok(f"{TABLA} eliminada")

    print("\n✅ DEMOSTRACIÓN COMPLETA — Iceberg cubre los 3 features (ACID, Time Travel, Schema Evolution)")
    print("   y se evidenciaron al menos 2 lotes append en una tabla Bronze (rúbrica § 3.1 + § 4.6.1).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
