"""
Smoke test · MongoDB

Verifica que MongoDB acepta conexiones autenticadas y opera CRUD básico.
Es la primera mitad del camino streaming (la otra mitad se valida en Sprint 2
cuando entren Kafka y Flink).

Ejecutar:
    make smoke-mongo
"""

import os
import sys

try:
    from pymongo import MongoClient
except ImportError:
    print("❌ pymongo no está instalado en el contenedor de Spark.")
    print("   Sprint 0 lo deja como dependencia opcional; instálalo con:")
    print("   docker compose exec spark-iceberg pip install pymongo")
    sys.exit(10)


def main() -> int:
    user = os.environ.get("MONGO_INITDB_ROOT_USERNAME", "admin")
    pwd = os.environ.get("MONGO_INITDB_ROOT_PASSWORD", "admin12345")
    host = os.environ.get("MONGO_HOST", "mongodb")
    port = int(os.environ.get("MONGO_PORT", "27017"))

    uri = f"mongodb://{user}:{pwd}@{host}:{port}/?authSource=admin"

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as exc:
        print(f"❌ No se pudo conectar a MongoDB en {host}:{port}: {exc}")
        return 1

    print(f"✓ MongoDB responde en {host}:{port}.")

    db = client["pulsomed"]
    coll = db["_sprint0_smoke"]

    try:
        coll.delete_many({})
        coll.insert_many([
            {"fuente": "siata", "pm25": 32.5, "ok": True},
            {"fuente": "metro", "afluencia": 1280, "ok": True},
        ])
        n_docs = coll.count_documents({"ok": True})
        if n_docs != 2:
            print(f"❌ Se esperaban 2 docs, se leyeron {n_docs}.")
            return 2

        sample = coll.find_one({"fuente": "siata"})
        print(f"✓ Inserción y lectura OK. Doc de muestra: {sample}")

        coll.drop()
        print("✓ Colección de prueba eliminada.")
    except Exception as exc:
        print(f"❌ Operación CRUD falló: {exc}")
        return 3

    print("✅ Smoke MongoDB pasó.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
