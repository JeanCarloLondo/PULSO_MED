"""
config.py — Constantes centralizadas y fábrica de SparkSession para Pulso Medellín.

Importar en todos los scripts de batch y streaming:
    from shared.config import crear_spark_session, TBL_BRONZE_MEDATA, ...

Nunca escribir strings de catálogo, rutas o nombres de colecciones fuera de este archivo.
"""

import os
# pyspark se importa dentro de crear_spark_session para que este módulo
# pueda usarse desde contenedores sin Spark instalado (ej. stream-runner).

# ── Iceberg REST Catalog ─────────────────────────────────────────────────────
CATALOG        = "demo"                   # catálogo pre-configurado en tabulario/spark-iceberg
REST_URI       = "http://iceberg-rest:8181"
WAREHOUSE_PATH = "s3://warehouse/"
MINIO_ENDPOINT = "http://minio:9000"

# ── Namespaces Medallion ─────────────────────────────────────────────────────
NS_TOP    = "pulsomed"
NS_BRONZE = "pulsomed.bronze"
NS_SILVER = "pulsomed.silver"
NS_GOLD   = "pulsomed.gold"

# ── Tablas Bronze ────────────────────────────────────────────────────────────
TBL_BRONZE_MEDATA      = f"{CATALOG}.{NS_BRONZE}.medata_incidentes"
TBL_BRONZE_METRO       = f"{CATALOG}.{NS_BRONZE}.metro_afluencia"
TBL_BRONZE_ENCICLA     = f"{CATALOG}.{NS_BRONZE}.encicla_prestamos"
TBL_BRONZE_SIATA       = f"{CATALOG}.{NS_BRONZE}.siata_lecturas"
TBL_BRONZE_GEOMEDELLIN = f"{CATALOG}.{NS_BRONZE}.geomedellin_comunas"
TBL_BRONZE_SIMM        = f"{CATALOG}.{NS_BRONZE}.simm_aforos"

# ── Tablas Silver ────────────────────────────────────────────────────────────
TBL_SILVER_INCIDENTES = f"{CATALOG}.{NS_SILVER}.incidentes_geocodificados"
TBL_SILVER_AFLUENCIA  = f"{CATALOG}.{NS_SILVER}.afluencia_horaria"
TBL_SILVER_ENCICLA    = f"{CATALOG}.{NS_SILVER}.viajes_encicla_anonimizados"
TBL_SILVER_AIRE       = f"{CATALOG}.{NS_SILVER}.lecturas_aire_validas"
TBL_SILVER_AFOROS     = f"{CATALOG}.{NS_SILVER}.aforos_corredor_geo"

# ── Tablas Gold ──────────────────────────────────────────────────────────────
TBL_GOLD_AFLUENCIA_PM25    = f"{CATALOG}.{NS_GOLD}.afluencia_vs_pm25"
TBL_GOLD_ACCIDENTALIDAD    = f"{CATALOG}.{NS_GOLD}.accidentalidad_por_comuna"
TBL_GOLD_ENCICLA_CLIMA     = f"{CATALOG}.{NS_GOLD}.demanda_encicla_vs_clima"
TBL_GOLD_CORREDORES_RIESGO = f"{CATALOG}.{NS_GOLD}.corredores_riesgo_compuesto"
# Gold derivada (Sprint 4): percentiles de afluencia Metro consumida en vivo
# por el job híbrido vía PyIceberg, reemplazando el JSON precomputado.
TBL_GOLD_PERCENTILES_METRO = f"{CATALOG}.{NS_GOLD}.percentiles_metro"

# ── Tablas Gold · Sprint 5 ───────────────────────────────────────────────────
# Módulo 06a — MLlib: métricas de evaluación del modelo de fatalidad
TBL_GOLD_ML_FATALIDAD_EVAL = f"{CATALOG}.{NS_GOLD}.ml_fatalidad_evaluacion"
# Módulo 06b — GraphFrames: centralidad PageRank de estaciones Metro
TBL_GOLD_RED_METRO_PAGERANK = f"{CATALOG}.{NS_GOLD}.red_metro_pagerank"
# Módulo 06b — GraphFrames: rutas óptimas entre pares de estaciones Metro
TBL_GOLD_RED_METRO_RUTAS    = f"{CATALOG}.{NS_GOLD}.red_metro_rutas_optimas"

# ── Kafka Topics (Sprint 2+) ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SIATA        = "siata.lecturas"
TOPIC_METRO        = "metro.validaciones"
TOPIC_ENCICLA      = "encicla.disponibilidad"
TOPIC_SIMM         = "simm.aforos"
KAFKA_PARTITIONS   = 2
KAFKA_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 días en milisegundos

# ── MongoDB ──────────────────────────────────────────────────────────────────
_mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
_mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin12345")
_mongo_host = os.getenv("MONGO_HOST", "mongodb")
_mongo_port = os.getenv("MONGO_PORT", "27017")

MONGO_DB            = "pulsomed"
MONGO_URI           = f"mongodb://{_mongo_user}:{_mongo_pass}@{_mongo_host}:{_mongo_port}/{MONGO_DB}?authSource=admin"
COL_ALERTAS_AIRE    = "alertas_aire"
COL_ENCICLA_DISP    = "disponibilidad_encicla"
COL_METRO_RT        = "afluencia_metro_rt"
COL_AFOROS_CORREDOR = "aforos_corredor"

# ── Columnas de auditoría Bronze ─────────────────────────────────────────────
COL_TIMESTAMP_INGESTA = "timestamp_ingesta"
COL_NOMBRE_ARCHIVO    = "nombre_archivo"
COL_FUENTE_ID         = "fuente_id"
COL_FECHA_INGESTA     = "fecha_ingesta"

# ── Calidad de datos ─────────────────────────────────────────────────────────
# SIATA usa -999 como centinela de nulo en PM2.5, PM10, temperatura, etc.
SIATA_NULL_SENTINEL = -999.0

# ── Privacidad EnCicla (Ley 1581) ────────────────────────────────────────────
# La clave HMAC vive en la variable de entorno; nunca en código ni en notebooks.
HMAC_SECRET = os.getenv("HMAC_USER_PSEUDO_SECRET", "")

# ── Sistemas de referencia de coordenadas ───────────────────────────────────
CRS_WGS84    = "EPSG:4326"
CRS_COLOMBIA = "EPSG:3116"  # Colombia Bogotá / Transverse Mercator


def crear_spark_session(nombre_app: str):
    """
    Devuelve una SparkSession apuntando al REST Catalog de Iceberg y a MinIO.
    Llamar una sola vez por proceso. Todos los scripts batch deben usar esta función.
    """
    from pyspark.sql import SparkSession  # import diferido (ver header)
    return (
        SparkSession.builder
        .appName(nombre_app)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE_PATH)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .getOrCreate()
    )
