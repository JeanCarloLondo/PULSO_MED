"""
init_iceberg_namespaces.py — Crea los namespaces Medallion en el catálogo Iceberg.

Ejecutar UNA sola vez, después de `make up` y antes de cualquier ingesta Bronze:
    make init-namespaces
"""

import sys
sys.path.insert(0, "/workspace/src")

import time

from shared.config import CATALOG, NS_TOP, NS_BRONZE, NS_SILVER, NS_GOLD, crear_spark_session

NAMESPACES_EN_ORDEN = [NS_TOP, NS_BRONZE, NS_SILVER, NS_GOLD]


def main() -> int:
    print("→ Inicializando SparkSession...")
    spark = crear_spark_session("PulsoMedellin-Init-Namespaces")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Reintentar por si el REST Catalog aún está arrancando
        for intento in range(1, 11):
            try:
                spark.sql(f"SHOW NAMESPACES IN {CATALOG}")
                break
            except Exception:
                print(f"  intento {intento}/10: REST Catalog no responde aún, esperando 3 s...")
                time.sleep(3)
        else:
            print("❌ REST Catalog no disponible tras 30 s.")
            return 1

        for ns in NAMESPACES_EN_ORDEN:
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
            print(f"  ✓ {CATALOG}.{ns}")

        print()
        print("✅ Namespaces Medallion listos. Puede iniciar la ingesta Bronze.")
        return 0

    except Exception as exc:
        print(f"❌ Error creando namespaces: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
