"""
Smoke test - MinIO + sanity-check del REST Catalog

Verifica:
  1. MinIO esta vivo y el bucket "warehouse" existe.
  2. El Iceberg REST Catalog responde en http://iceberg-rest:8181/v1/config.

Ejecutar:
    make smoke-minio
"""

import os
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET


def main() -> int:
    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    bucket = "warehouse"
    rest_url = "http://iceberg-rest:8181/v1/config"

    if not access_key or not secret_key:
        print(
            "ERROR: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY no estan "
            "definidas dentro del contenedor de Spark. Revisa docker-compose.yml."
        )
        return 1

    # El contenedor mc deja el bucket `warehouse` con acceso anonimo publico.
    # Usamos HTTP S3 para evitar depender de boto3 en tabulario/spark-iceberg.
    bucket_url = f"{endpoint.rstrip('/')}/{bucket}/?list-type=2"
    try:
        with urllib.request.urlopen(bucket_url, timeout=5) as resp:
            body = resp.read()
    except Exception as exc:
        print(f"ERROR: no se pudo leer el bucket '{bucket}' en MinIO ({bucket_url}): {exc}")
        print("       Revisa: make logs SERVICE=mc")
        return 2

    try:
        root = ET.fromstring(body)
        bucket_name = root.findtext("{http://s3.amazonaws.com/doc/2006-03-01/}Name")
    except ET.ParseError as exc:
        print(f"ERROR: MinIO respondio, pero la respuesta no fue XML S3 valido: {exc}")
        return 3

    if bucket_name != bucket:
        print(f"ERROR: se esperaba bucket '{bucket}' y llego '{bucket_name}'.")
        print("       Revisa: make logs SERVICE=mc")
        return 3

    print(f"OK: MinIO responde en {endpoint}. Bucket '{bucket}' existe y es legible.")

    # Reintenta porque el REST Catalog tarda ~10-30s en levantar y no tiene
    # healthcheck (su imagen no trae wget/curl/nc).
    print(f"Esperando al Iceberg REST Catalog en {rest_url} ...")
    last_err = None
    for intento in range(1, 31):  # ~60s maximo
        try:
            with urllib.request.urlopen(rest_url, timeout=2) as resp:
                if resp.status == 200:
                    print(f"OK: REST Catalog responde (intento {intento}).")
                    break
        except urllib.error.URLError as exc:
            last_err = exc
            time.sleep(2)
    else:
        print(f"ERROR: REST Catalog no respondio tras 30 intentos. Ultimo error: {last_err}")
        print("       Revisa: make logs SERVICE=iceberg-rest")
        return 4

    print("OK: Smoke MinIO + REST Catalog paso.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
