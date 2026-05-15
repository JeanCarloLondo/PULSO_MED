"""
siata_a_iceberg_streaming.py — Bonus +1 de la rúbrica § 3.2:
**Spark Structured Streaming** como alternativa/complemento a Flink.

Lee el tópico `siata.lecturas` desde Kafka en modo streaming y lo persiste
en una tabla Iceberg Bronze (`demo.pulsomed.bronze.siata_streaming`) usando
micro-batches de 30 segundos. Esto demuestra:

  · Spark Structured Streaming consumiendo de Kafka.
  · Sink Iceberg en streaming (Append mode), con checkpoint a MinIO.
  · El mismo dato que el job Flink puede aterrizar en el Lakehouse — los
    dos motores conviven sin pisarse (cada uno escribe a tablas/colecciones
    distintas).

Cómo correr:
    docker compose exec -T spark-iceberg spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        /workspace/src/streaming/structured/siata_a_iceberg_streaming.py
    # equivalente:  make stream-structured-siata

Variables:
    KAFKA_BOOTSTRAP_SERVERS   default kafka:9092
    DURACION_S                default 0  (0 = correr indefinidamente)
    MICRO_BATCH_S             default 30 segundos por trigger
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from shared.config import CATALOG, KAFKA_BOOTSTRAP, NS_BRONZE, crear_spark_session

TOPIC = "siata.lecturas"
TBL_DESTINO = f"{CATALOG}.{NS_BRONZE}.siata_streaming"
CHECKPOINT = "s3://warehouse/_checkpoints/siata_streaming"

MICRO_BATCH_S = int(os.getenv("MICRO_BATCH_S", "30"))
DURACION_S = int(os.getenv("DURACION_S", "0"))


# Esquema del mensaje JSON publicado por `siata_producer.py`
ESQUEMA_SIATA = StructType([
    StructField("estacion_id",     StringType()),
    StructField("estacion_nombre", StringType()),
    StructField("zona",            StringType()),
    StructField("latitud",         DoubleType()),
    StructField("longitud",        DoubleType()),
    StructField("timestamp",       StringType()),
    StructField("pm25",            DoubleType()),
    StructField("pm10",            DoubleType()),
])


def main() -> int:
    spark = crear_spark_session("Pulsomed-Structured-Streaming-SIATA")
    spark.sparkContext.setLogLevel("WARN")

    # Asegurar que la tabla destino exista (idempotente)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TBL_DESTINO} (
            estacion_id     STRING,
            estacion_nombre STRING,
            zona            STRING,
            latitud         DOUBLE,
            longitud        DOUBLE,
            event_ts        TIMESTAMP,
            pm25            DOUBLE,
            pm10            DOUBLE,
            ingest_ts       TIMESTAMP,
            kafka_offset    LONG,
            kafka_partition INT
        ) USING iceberg
        """
    )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.from_json(F.col("value").cast("string"), ESQUEMA_SIATA).alias("d"),
        )
        .select(
            F.col("d.estacion_id").alias("estacion_id"),
            F.col("d.estacion_nombre").alias("estacion_nombre"),
            F.col("d.zona").alias("zona"),
            F.col("d.latitud").alias("latitud"),
            F.col("d.longitud").alias("longitud"),
            F.to_timestamp(F.col("d.timestamp")).alias("event_ts"),
            F.col("d.pm25").alias("pm25"),
            F.col("d.pm10").alias("pm10"),
            F.current_timestamp().alias("ingest_ts"),
            "kafka_offset",
            "kafka_partition",
        )
        .filter(F.col("estacion_id").isNotNull())
    )

    query = (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("path", TBL_DESTINO)
        .trigger(processingTime=f"{MICRO_BATCH_S} seconds")
        .toTable(TBL_DESTINO)
    )

    print(
        f"→ Structured Streaming activo:  {TOPIC} → {TBL_DESTINO}  "
        f"(trigger cada {MICRO_BATCH_S}s, checkpoint en MinIO)",
        flush=True,
    )

    if DURACION_S > 0:
        query.awaitTermination(DURACION_S)
        query.stop()
    else:
        query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
