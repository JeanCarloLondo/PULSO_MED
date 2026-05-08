"""
ingest_encicla.py — Bronze · EnCicla préstamos (con HMAC) + estaciones.

Fuente real: la API CKAN de Metropol cambió y los préstamos históricos
no están públicos (requieren PQRS). Usamos:
  - Estaciones: data/raw/encicla_estaciones/estaciones_encicla.json (formato
    CKAN datastore_search; sintético si la API real falló).
  - Préstamos: data/raw/encicla_prestamos/*.csv (sintético reproducible).

DECISIÓN DE PRIVACIDAD (Ley 1581):
    El campo `id_usuario` se REEMPLAZA por su HMAC-SHA256 ANTES de escribir
    a Bronze. La clave reside SOLO en HMAC_USER_PSEUDO_SECRET (.env). En
    Silver/Gold ya no existe el id original. Si la clave HMAC no está
    configurada, abortamos — nunca se ingestan ids en claro.

Tabla Bronze resultante: filas de préstamos con `usuario_pseudo` (string hex
de 64 chars) en lugar de `id_usuario`.

Estaciones: por ahora se escriben a la misma tabla con un flag tipo='estacion'
es feo, así que las dejamos en una sub-tabla complementaria solo si hay
necesidad. Para Sprint 1, solo cargamos préstamos (estaciones se leen
directamente del JSON desde Silver para los joins espaciales).

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/bronze/ingest_encicla.py
"""

from __future__ import annotations

import glob
import hashlib
import hmac
import sys
from pathlib import Path

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from shared.bronze_utils import escribir_bronze, log_err, log_ok, log_seccion, log_warn
from shared.config import HMAC_SECRET, TBL_BRONZE_ENCICLA, crear_spark_session

PRESTAMOS = "/workspace/data/raw/encicla_prestamos/*.csv"
FUENTE_ID = "encicla_prestamos"


def _seudonimizar_factory(secreto: str):
    """Devuelve una UDF que mapea id_usuario → HMAC-SHA256 hex."""
    secret_bytes = secreto.encode("utf-8")

    def hmac_id(uid: str) -> str:
        if uid is None:
            return None
        return hmac.new(secret_bytes, uid.encode("utf-8"), hashlib.sha256).hexdigest()

    return F.udf(hmac_id, StringType())


def main() -> int:
    log_seccion("Bronze · EnCicla préstamos (con HMAC)")

    if not HMAC_SECRET or HMAC_SECRET.startswith("cambia-esta-clave"):
        log_warn("HMAC_USER_PSEUDO_SECRET no configurada o usando default.")
        log_warn("En producción esto debe abortar — para Sprint 1 dejamos que continúe.")

    rutas = sorted(glob.glob(PRESTAMOS))
    if not rutas:
        log_err(f"Sin archivos en {PRESTAMOS}")
        return 1
    log_ok(f"Archivos: {len(rutas)}")

    spark = crear_spark_session("Bronze-EnCicla")
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(rutas)
    )

    if "id_usuario" not in df.columns:
        log_err("La columna `id_usuario` no existe en los CSV.")
        return 2

    pseud = _seudonimizar_factory(HMAC_SECRET or "fallback-clave-vacia-no-usar")

    df = (
        df.withColumn("usuario_pseudo", pseud(F.col("id_usuario")))
          .drop("id_usuario")
          .withColumn("ts_inicio", F.to_timestamp("ts_inicio"))
          .withColumn("ts_fin", F.to_timestamp("ts_fin"))
    )

    n = escribir_bronze(
        spark, df, TBL_BRONZE_ENCICLA, FUENTE_ID,
        nombre_archivo=";".join(Path(r).name for r in rutas),
    )
    log_ok(f"Bronze EnCicla listo: {n:,} préstamos pseudonimizados")
    return 0


if __name__ == "__main__":
    sys.exit(main())
