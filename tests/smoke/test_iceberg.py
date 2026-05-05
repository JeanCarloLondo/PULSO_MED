"""
Smoke test · Iceberg + Spark + REST Catalog

Valida la integración completa del Lakehouse:
    1. Spark se conecta al REST Catalog (puerto 8181 dentro de la red).
    2. El REST Catalog escribe metadatos en MinIO (bucket `warehouse`).
    3. Spark crea una tabla Iceberg, le inserta filas, las lee, y la dropea.

Si este test pasa, MinIO + REST Catalog + Spark + Iceberg están todos sanos
y el equipo puede empezar el Sprint 1 (ingesta a Bronze) con confianza.

Ejecutar:
    make smoke-iceberg
"""

import sys
import time

from pyspark.sql import SparkSession


CATALOG = "demo"  # tabulario/spark-iceberg ya define este catálogo apuntando al REST
NS = "smoke"
TABLE = f"{CATALOG}.{NS}._sprint0_test"


def get_spark() -> SparkSession:
    # En la imagen tabulario/spark-iceberg el catálogo `demo` ya viene cableado
    # al REST Catalog vía variables de entorno. No hace falta pasarlo en .config().
    return (
        SparkSession.builder
        .appName("PulsoMedellin-Sprint0-Smoke")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "rest")
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .getOrCreate()
    )


def main() -> int:
    print("→ Inicializando SparkSession (puede tardar 10-20 s la primera vez)...")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Reintentar por si el REST Catalog aún no está listo cuando Spark arranca.
        # Spark depende de iceberg-rest con service_started (no service_healthy)
        # porque la imagen no permite healthcheck.
        last_err = None
        for intento in range(1, 11):
            try:
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NS}")
                break
            except Exception as exc:
                last_err = exc
                print(f"  intento {intento}/10: REST Catalog aún no responde, esperando 3s...")
                time.sleep(3)
        else:
            print(f"❌ No se pudo crear namespace tras 10 intentos: {last_err}")
            return 1

        print(f"✓ Namespace {CATALOG}.{NS} listo.")

        print(f"→ Creando tabla Iceberg {TABLE}...")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {TABLE} (
                id BIGINT,
                fuente STRING,
                creado_en TIMESTAMP
            ) USING iceberg
            """
        )

        print("→ Insertando 3 filas de prueba...")
        spark.sql(
            f"""
            INSERT INTO {TABLE} VALUES
              (1, 'siata',  current_timestamp()),
              (2, 'metro',  current_timestamp()),
              (3, 'medata', current_timestamp())
            """
        )

        print("→ Leyendo de vuelta...")
        df = spark.table(TABLE)
        n = df.count()
        df.show(truncate=False)

        if n != 3:
            print(f"❌ Se esperaban 3 filas, se leyeron {n}.")
            return 2

        print("→ Verificando snapshot history (Time Travel disponible)...")
        snaps = spark.sql(f"SELECT count(*) AS n FROM {TABLE}.history").first()["n"]
        print(f"   snapshots actuales: {snaps}")

        print("→ Dropping tabla de prueba para no contaminar el catálogo...")
        spark.sql(f"DROP TABLE {TABLE} PURGE")

        print()
        print("✅ Smoke Iceberg pasó: MinIO + REST + Spark + Iceberg están vivos.")
        return 0

    except Exception as exc:
        print(f"❌ Smoke Iceberg falló: {exc}")
        return 3
    finally:
        time.sleep(0.2)


if __name__ == "__main__":
    sys.exit(main())
