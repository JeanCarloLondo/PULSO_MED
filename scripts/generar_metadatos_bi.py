#!/usr/bin/env python3
"""
generar_metadatos_bi.py · Sprint 7 (BI)

Materializa los metadatos del proyecto Pulso Medellín en cuatro tablas
queryables por Apache Superset:

    - hallazgos              · descubrimientos batch (B-1..B-4) y streaming
                                (S-1..S-4) + pregunta híbrida 4.3 + ML/Grafo.
    - decisiones             · ADRs firmados (02, 04, 05, 07) + decisiones
                                grandes del Sprint 6 cumplimiento de rúbrica.
    - herramientas           · stack tecnológico usado, capa, razón de uso,
                                alternativa descartada.
    - cumplimiento_rubrica   · checklist contra `docs/Proyecto_Final_ST1630.pdf`
                                con § rúbrica, puntos, estado y evidencia.

Salida:
    data/processed/bi/hallazgos.csv
    data/processed/bi/decisiones.csv
    data/processed/bi/herramientas.csv
    data/processed/bi/cumplimiento_rubrica.csv
    data/processed/bi/pulsomed_bi.db    (SQLite con las 4 tablas)

Uso:
    python3 scripts/generar_metadatos_bi.py

El script corre en el host (no requiere Spark ni Docker). Sólo necesita la
stdlib de Python 3.10+.
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DESTINO = RAIZ / "data" / "processed" / "bi"


# ----------------------------------------------------------------------
# 1. Hallazgos (preguntas de negocio respondidas por el proyecto)
# ----------------------------------------------------------------------

HALLAZGOS: list[dict] = [
    # ----- Batch B-1..B-4 -----
    {
        "id": "B-1",
        "categoria": "Batch analítico",
        "fuente": "afluencia_vs_pm25 (Gold)",
        "pregunta": "¿Cómo se correlacionan PM2.5 y lluvia con la afluencia del Metro?",
        "hallazgo": "Correlación negativa de PM2.5 con validaciones Metro a nivel estación×mes; lluvia reduce afluencia en líneas alimentadoras del Metrocable.",
        "tabla_gold": "demo.pulsomed.gold.afluencia_vs_pm25",
        "modulo_curso": "Lakehouse + Iceberg",
        "sprint": 1,
    },
    {
        "id": "B-2",
        "categoria": "Batch analítico",
        "fuente": "accidentalidad_por_comuna (Gold)",
        "pregunta": "¿Qué comunas concentran la mayor severidad vial?",
        "hallazgo": "Indice de severidad ponderado (muertos·5 + heridos·1) revela top-5 comunas con concentración 4× sobre la media; pivot por gravedad año a año.",
        "tabla_gold": "demo.pulsomed.gold.accidentalidad_por_comuna",
        "modulo_curso": "Lakehouse + Iceberg",
        "sprint": 1,
    },
    {
        "id": "B-3",
        "categoria": "Batch analítico",
        "fuente": "demanda_encicla_vs_clima (Gold)",
        "pregunta": "¿Cuál es la elasticidad de la demanda EnCicla frente a temperatura, lluvia y PM2.5?",
        "hallazgo": "Demanda cae ~30% en días con lluvia y ~15% en días con PM2.5>50 µg/m³; binning por temperatura y PM2.5 muestra el rango óptimo de uso.",
        "tabla_gold": "demo.pulsomed.gold.demanda_encicla_vs_clima",
        "modulo_curso": "Lakehouse + Iceberg",
        "sprint": 1,
    },
    {
        "id": "B-4",
        "categoria": "Batch analítico",
        "fuente": "corredores_riesgo_compuesto (Gold)",
        "pregunta": "¿Qué corredores combinan alto volumen + alta severidad?",
        "hallazgo": "Ranking compuesto por comuna identifica corredores prioritarios para inversión vial (volumen + severidad ponderada).",
        "tabla_gold": "demo.pulsomed.gold.corredores_riesgo_compuesto",
        "modulo_curso": "Lakehouse + Iceberg",
        "sprint": 1,
    },
    # ----- Streaming S-1..S-4 -----
    {
        "id": "S-1",
        "categoria": "Streaming operacional",
        "fuente": "Kafka encicla.disponibilidad + ventana sliding 1m/30s",
        "pregunta": "¿Qué estaciones EnCicla están con stock crítico ahora mismo?",
        "hallazgo": "Job sliding (1 min ventana, 30 s slide) detecta estaciones con ≤UMBRAL_BICIS bicis disponibles; alerta inmediata por estación.",
        "tabla_gold": "mongodb.pulsomed.encicla_alertas",
        "modulo_curso": "Streaming Kafka + ventanas",
        "sprint": 3,
    },
    {
        "id": "S-2",
        "categoria": "Streaming operacional",
        "fuente": "Kafka siata.lecturas + ventana tumbling 10m",
        "pregunta": "¿Cuándo y dónde PM2.5 cruza el umbral de la OMS?",
        "hallazgo": "Tumbling 10 min agrega por zona; cuando promedio>UMBRAL_PM25 emite alerta a Mongo. Mismo flujo es consumido por Flink real (rúbrica § 4.4).",
        "tabla_gold": "mongodb.pulsomed.alertas_aire",
        "modulo_curso": "Streaming Kafka + Flink",
        "sprint": 2,
    },
    {
        "id": "S-3",
        "categoria": "Streaming operacional",
        "fuente": "Kafka simm.aforos + ventana tumbling 5m",
        "pregunta": "¿Qué corredor SIMM tiene aforos por encima del p90 esperado?",
        "hallazgo": "Tumbling 5 min agrega aforos por corredor; cruza con corredores_alta_siniestralidad.json para alertar congestión en zonas de riesgo.",
        "tabla_gold": "mongodb.pulsomed.simm_aforos_rt",
        "modulo_curso": "Streaming Kafka + ventanas",
        "sprint": 3,
    },
    {
        "id": "S-4",
        "categoria": "Streaming operacional",
        "fuente": "Kafka metro.afluencia + ventana tumbling 5m",
        "pregunta": "¿Cómo evoluciona la afluencia Metro en tiempo real por línea?",
        "hallazgo": "Tumbling 5 min agrega pasajeros por línea×hora; sirve de input al job híbrido 4.3 para comparar contra percentiles batch.",
        "tabla_gold": "mongodb.pulsomed.metro_afluencia_rt",
        "modulo_curso": "Streaming Kafka + ventanas",
        "sprint": 3,
    },
    # ----- Híbrida 4.3 -----
    {
        "id": "4.3",
        "categoria": "Híbrido batch↔streaming",
        "fuente": "Kafka metro.afluencia + Gold.percentiles_metro vía PyIceberg",
        "pregunta": "¿Cuando llueve, la afluencia Metro RT cae bajo el p90 histórico para esa franja?",
        "hallazgo": "Job híbrido lee percentiles desde Iceberg en vivo (fallback a JSON precomputado) y los compara contra el stream; materializa Lambda explícito.",
        "tabla_gold": "demo.pulsomed.gold.percentiles_metro",
        "modulo_curso": "Integración batch+streaming",
        "sprint": 4,
    },
    # ----- ML + Grafo Sprint 5 -----
    {
        "id": "ML-1",
        "categoria": "ML supervisado",
        "fuente": "MLlib RandomForest multiclase",
        "pregunta": "¿Podemos predecir la gravedad de un incidente vial (solo daños / heridos / muertos)?",
        "hallazgo": "RandomForestClassifier multiclase entrenado sobre Silver.incidentes_geocodificados; métricas (accuracy / F1 / matriz de confusión) en notebook 02.",
        "tabla_gold": "data/processed/modelos/fatalidad_rf/",
        "modulo_curso": "06a · Spark MLlib",
        "sprint": 5,
    },
    {
        "id": "G-1",
        "categoria": "Grafo",
        "fuente": "GraphFrames / NetworkX red Metro",
        "pregunta": "¿Cuáles son las estaciones Metro más centrales y las rutas óptimas?",
        "hallazgo": "PageRank ordena estaciones por centralidad; Dijkstra calcula rutas mínimas entre todos los pares (red_metro_rutas_optimas).",
        "tabla_gold": "demo.pulsomed.gold.red_metro_pagerank",
        "modulo_curso": "06b · GraphX / GraphFrames",
        "sprint": 5,
    },
    # ----- Rúbrica Sprint 6 -----
    {
        "id": "ICE-1",
        "categoria": "Lakehouse features",
        "fuente": "scripts/demo_iceberg_features.py",
        "pregunta": "¿Iceberg soporta realmente ACID + Time Travel + Schema Evolution?",
        "hallazgo": "Demo verifica los 3: 2 lotes append generan 2 snapshots distintos; VERSION AS OF lee snapshot 1; ALTER TABLE ADD COLUMN deja nulos en filas viejas y valores en nuevas — sin reescritura.",
        "tabla_gold": "demo.pulsomed.bronze._features_demo (transitoria)",
        "modulo_curso": "Lakehouse + Iceberg",
        "sprint": 6,
    },
]


# ----------------------------------------------------------------------
# 2. Decisiones técnicas (ADRs firmados + Sprint 6)
# ----------------------------------------------------------------------

DECISIONES: list[dict] = [
    {
        "id": "ADR-02",
        "titulo": "Arquitectura Lambda vs Kappa",
        "estado": "Aceptado · revisado 2026-05-15",
        "decision": "Lambda explícito: batch (Iceberg) + streaming (Kafka→Mongo) sincronizados por capa servidora.",
        "alternativa_descartada": "Kappa puro (reprocesar histórico desde Kafka): demasiado costoso para 6 fuentes con SLAs heterogéneos.",
        "motivacion": "Tres clases de consulta incompatibles bajo un solo paradigma (analítica histórica vs operacional vs híbrida).",
        "modulo_curso": "02 — Lambda vs Kappa",
        "archivo": "docs/decisiones/02-lambda-vs-kappa.md",
    },
    {
        "id": "ADR-04",
        "titulo": "Benchmark CSV vs Parquet vs Parquet+ZSTD",
        "estado": "Aceptado",
        "decision": "Iceberg sobre Parquet con codec ZSTD nivel 3 (default).",
        "alternativa_descartada": "Parquet+Snappy (default Spark) y CSV crudo.",
        "motivacion": "ZSTD comprime ~10% más que Snappy con costo CPU comparable; lectura columnar 5-20× más rápida que CSV. Benchmark reproducible sobre 270k filas MEData + 1.2M filas SIATA.",
        "modulo_curso": "04 — Formatos columnares",
        "archivo": "docs/decisiones/04-benchmark-formatos.md",
    },
    {
        "id": "ADR-05",
        "titulo": "Delta Lake vs Apache Iceberg",
        "estado": "Aceptado",
        "decision": "Apache Iceberg con catálogo REST (tabulario/iceberg-rest) y warehouse S3-compatible (MinIO).",
        "alternativa_descartada": "Delta Lake (mejor integración Databricks, peor multi-motor en 2026).",
        "motivacion": "Iceberg es lengua franca multi-motor: misma tabla legible desde Spark, Trino, Snowflake, Athena, Flink. Delta sigue siendo Spark-centric. REST Catalog evita Hive Metastore.",
        "modulo_curso": "05 — Lakehouse",
        "archivo": "docs/decisiones/05-delta-vs-iceberg.md",
    },
    {
        "id": "ADR-07",
        "titulo": "Cloud AWS vs GCP + Gobernanza Ley 1581/1712",
        "estado": "Aceptado",
        "decision": "AWS (S3 + Glue + EMR Serverless + MSK + Atlas) para puesta en producción; HMAC-SHA256 sobre id_usuario EnCicla antes de Bronze.",
        "alternativa_descartada": "GCP (BigLake + Dataproc + Pub/Sub): integración Iceberg más reciente, menos maduro en 2026.",
        "motivacion": "S3 es la implementación de referencia de Iceberg; Glue soporta REST Catalog nativo; IAM cubre controles por capa Medallion. Ley 1712 (transparencia) + Ley 1581 (datos personales).",
        "modulo_curso": "07 — Cloud + Gobernanza",
        "archivo": "docs/decisiones/07-cloud-aws-vs-gcp.md",
    },
    {
        "id": "S6-Flink",
        "titulo": "Mantener stream-runner Python + agregar Flink real",
        "estado": "Aceptado (Sprint 6 cumplimiento rúbrica)",
        "decision": "Cluster Flink JobManager+TaskManager con job PyFlink (checkpointing at-least-once cada 60s) coexistiendo con los 4 jobs Python.",
        "alternativa_descartada": "Migrar todo el streaming a Flink real (duplicaba el trabajo sin valor adicional para evaluación).",
        "motivacion": "Rúbrica § 4.4 pide UN job Flink con las 4 features (Kafka, ventana, sink NoSQL, checkpointing). Los jobs Python cubren las preguntas S-1..S-4 con menos overhead.",
        "modulo_curso": "Rúbrica § 3.1 + § 4.4",
        "archivo": "docs/sprints/sprint-6-cumplimiento-rubrica.md",
    },
    {
        "id": "S6-IcebergFeatures",
        "titulo": "Demo verificable de ACID + Time Travel + Schema Evolution",
        "estado": "Aceptado (Sprint 6)",
        "decision": "Script único que valida los 3 features en una tabla transitoria, retorna exit-code 0 si pasan.",
        "alternativa_descartada": "Documentación textual sin código ejecutable.",
        "motivacion": "Rúbrica § 4.6.4 exige evidencia REPRODUCIBLE de las 3 características. La demo programática es auditable.",
        "modulo_curso": "Rúbrica § 4.6.4",
        "archivo": "scripts/demo_iceberg_features.py",
    },
    {
        "id": "S6-KafkaTopics",
        "titulo": "Tópicos Kafka con ≥2 particiones y retención 7 días",
        "estado": "Aceptado (Sprint 6)",
        "decision": "Script init_kafka_topics.py via KafkaAdminClient: 2 particiones + retention.ms=604800000.",
        "alternativa_descartada": "Auto-create con defaults (1 partición, retención infinita).",
        "motivacion": "Rúbrica § 4.3.2 exige paralelismo (≥2 particiones) y política de retención explícita.",
        "modulo_curso": "Rúbrica § 4.3.2",
        "archivo": "scripts/init_kafka_topics.py",
    },
    {
        "id": "S7-BI",
        "titulo": "Apache Superset como herramienta BI para el informe final",
        "estado": "Aceptado (Sprint 7)",
        "decision": "Superset 4.0 (custom image con driver Trino) conectado a Trino para Gold + SQLite para metadatos del proyecto.",
        "alternativa_descartada": "Metabase (UI más simple pero menos potente en SQL Lab); Power BI (no integrable a Docker stack).",
        "motivacion": "Superset es BI open source profesional, integra nativamente con Trino, y deja todo el proyecto en un solo `docker compose up`.",
        "modulo_curso": "Entrega final · demo",
        "archivo": "docs/sprints/sprint-7-bi-superset.md",
    },
]


# ----------------------------------------------------------------------
# 3. Herramientas usadas (stack tecnológico)
# ----------------------------------------------------------------------

HERRAMIENTAS: list[dict] = [
    {
        "herramienta": "MinIO",
        "categoria": "Storage S3-compatible",
        "capa": "Infraestructura",
        "version": "latest (2026)",
        "razon_uso": "Implementación local del protocolo S3; sirve como warehouse físico de Iceberg sin depender de AWS.",
        "alternativa_descartada": "AWS S3 (costo + dependencia de internet para demos locales).",
        "sprint_introducido": 0,
    },
    {
        "herramienta": "Apache Iceberg",
        "categoria": "Table format",
        "capa": "Lakehouse",
        "version": "1.5+",
        "razon_uso": "ACID + time travel + schema evolution + interoperabilidad multi-motor (Spark/Trino/Flink). Decisión en ADR-05.",
        "alternativa_descartada": "Delta Lake (Spark-centric en 2026).",
        "sprint_introducido": 0,
    },
    {
        "herramienta": "Iceberg REST Catalog",
        "categoria": "Catalog service",
        "capa": "Lakehouse",
        "version": "tabulario/iceberg-rest latest",
        "razon_uso": "Catálogo sin Hive Metastore; lengua franca entre motores. Permite usar el mismo catálogo desde Spark, Trino y PyIceberg.",
        "alternativa_descartada": "Hive Metastore (pesado, JVM, complejo para demo).",
        "sprint_introducido": 0,
    },
    {
        "herramienta": "Apache Spark + PySpark",
        "categoria": "Procesamiento batch",
        "capa": "Compute",
        "version": "3.5 (tabulario/spark-iceberg)",
        "razon_uso": "Motor batch principal para Bronze→Silver→Gold con Iceberg pre-integrado. Imagen Tabular incluye AWS bundle + Jupyter.",
        "alternativa_descartada": "Dask (menor soporte Iceberg en 2026).",
        "sprint_introducido": 1,
    },
    {
        "herramienta": "Apache Kafka + Zookeeper",
        "categoria": "Message broker",
        "capa": "Streaming",
        "version": "Confluent 7.5",
        "razon_uso": "Bus de eventos canónico; soporta los 4 productores (siata, encicla, simm, metro) con 2 particiones por tópico.",
        "alternativa_descartada": "Redis Streams (menor garantía de orden + retención).",
        "sprint_introducido": 2,
    },
    {
        "herramienta": "Apache Flink + PyFlink",
        "categoria": "Streaming engine",
        "capa": "Streaming",
        "version": "1.18.1",
        "razon_uso": "Rúbrica § 4.4 exige Flink. Job PyFlink real con KafkaSource + TumblingWindow + MongoSink + checkpointing at-least-once.",
        "alternativa_descartada": "Spark Structured Streaming (también incluido como bonus, pero no cubre § 4.4).",
        "sprint_introducido": 6,
    },
    {
        "herramienta": "MongoDB",
        "categoria": "NoSQL (documental)",
        "capa": "Serving operacional",
        "version": "7.x",
        "razon_uso": "Sink rápido para alertas streaming y vistas operacionales (< 10 min historia). Decisión documentada en arquitectura.",
        "alternativa_descartada": "Redis (no persiste histórico) / Cassandra (overkill para volumen del proyecto).",
        "sprint_introducido": 2,
    },
    {
        "herramienta": "Trino",
        "categoria": "Motor SQL distribuido",
        "capa": "Consumo analítico",
        "version": "latest (462+)",
        "razon_uso": "Tercer motor SQL sobre las mismas tablas Iceberg (bonus +2pt). Demuestra interoperabilidad del lakehouse y alimenta Superset.",
        "alternativa_descartada": "Athena (cloud-only, no encaja en demo local).",
        "sprint_introducido": 5,
    },
    {
        "herramienta": "Apache Superset",
        "categoria": "Business Intelligence",
        "capa": "Consumo analítico",
        "version": "4.0.2",
        "razon_uso": "Herramienta BI para el informe/demo final. Se conecta a Trino (Gold) y SQLite (metadatos del proyecto).",
        "alternativa_descartada": "Metabase (UI más simple) / Power BI (no integrable a Docker).",
        "sprint_introducido": 7,
    },
    {
        "herramienta": "Streamlit",
        "categoria": "Dashboard tiempo real",
        "capa": "Consumo operacional",
        "version": "1.30+",
        "razon_uso": "Dashboard del Sprint 3 (5 paneles + mapa pydeck) refrescando cada 5s desde Mongo. No es BI, es app de monitoreo.",
        "alternativa_descartada": "Grafana (más pesado, requiere Prometheus para series temporales).",
        "sprint_introducido": 3,
    },
    {
        "herramienta": "mrjob (Hadoop MapReduce)",
        "categoria": "MapReduce legacy",
        "capa": "Compute legacy",
        "version": "0.7.4",
        "razon_uso": "Módulo 01 del curso exige MapReduce. Job mrjob normaliza CSV pre/post-2017 de MEData; salida ingresada a Bronze.",
        "alternativa_descartada": "Hadoop Streaming puro (más verboso, sin testing).",
        "sprint_introducido": 4,
    },
    {
        "herramienta": "Spark MLlib (RandomForest)",
        "categoria": "Machine Learning",
        "capa": "ML",
        "version": "Spark 3.5",
        "razon_uso": "Módulo 06a — predicción multiclase de gravedad de incidente vial sobre Silver geocodificado.",
        "alternativa_descartada": "scikit-learn (no escala más allá del nodo).",
        "sprint_introducido": 5,
    },
    {
        "herramienta": "GraphFrames / NetworkX",
        "categoria": "Grafos",
        "capa": "ML",
        "version": "0.8 / 3.x",
        "razon_uso": "Módulo 06b — PageRank + Dijkstra sobre red Metro (~30 estaciones). NetworkX para Dijkstra all-pairs; GraphFrames para PageRank a escala.",
        "alternativa_descartada": "GraphX puro (Scala, rompe convención Python).",
        "sprint_introducido": 5,
    },
    {
        "herramienta": "PyIceberg",
        "categoria": "Cliente Iceberg ligero",
        "capa": "Lakehouse",
        "version": "0.7+",
        "razon_uso": "Lee tablas Iceberg desde el stream-runner sin Spark; usado por job híbrido para consultar Gold en vivo.",
        "alternativa_descartada": "Levantar Spark dentro del stream-runner (overhead 5×).",
        "sprint_introducido": 4,
    },
    {
        "herramienta": "Docker Compose",
        "categoria": "Orquestación",
        "capa": "Infraestructura",
        "version": "v2",
        "razon_uso": "Stack completo reproducible en una sola red `pulsomed-net`. Rúbrica § 4.2 exige docker-compose.",
        "alternativa_descartada": "Kubernetes (overkill para demo académica).",
        "sprint_introducido": 0,
    },
    {
        "herramienta": "Make",
        "categoria": "Task runner",
        "capa": "Operaciones",
        "version": "GNU 4.x",
        "razon_uso": "Punto de entrada uniforme `make <target>` para todo el pipeline (Sprint 0→7). `make all` corre Sprint 0→5 end-to-end.",
        "alternativa_descartada": "Shell scripts sueltos (sin documentación auto-generada con --help).",
        "sprint_introducido": 0,
    },
]


# ----------------------------------------------------------------------
# 4. Cumplimiento de rúbrica
# ----------------------------------------------------------------------

CUMPLIMIENTO_RUBRICA: list[dict] = [
    {
        "seccion": "4.1",
        "titulo": "Descripción del problema",
        "puntos": 18,
        "estado": "Cumplido",
        "evidencia": "docs/Propuesta_pulsomed_SID.pdf + docs/01-arquitectura.md + 6 fuentes con esquemas reales",
    },
    {
        "seccion": "4.2",
        "titulo": "Arquitectura Docker",
        "puntos": 12,
        "estado": "Cumplido",
        "evidencia": "docker-compose.yml + diagrama Mermaid + README reproducible",
    },
    {
        "seccion": "4.3",
        "titulo": "Kafka (4 tópicos, 2 particiones, retención)",
        "puntos": 12,
        "estado": "Cumplido",
        "evidencia": "scripts/init_kafka_topics.py — 4 tópicos × 2 particiones × 7 días retención",
    },
    {
        "seccion": "4.4",
        "titulo": "Apache Flink (cluster real + checkpointing)",
        "puntos": 15,
        "estado": "Cumplido",
        "evidencia": "Cluster Flink 1.18 + src/streaming/flink_real/siata_alert_flink.py con AT_LEAST_ONCE",
    },
    {
        "seccion": "4.5",
        "titulo": "NoSQL (MongoDB con ≥5 colecciones)",
        "puntos": 12,
        "estado": "Cumplido",
        "evidencia": "5 colecciones Mongo + CLI consultar_alertas.py + Streamlit dashboard",
    },
    {
        "seccion": "4.6",
        "titulo": "Spark + Iceberg (ACID + Time Travel + Schema Evolution + 2 lotes)",
        "puntos": 15,
        "estado": "Cumplido",
        "evidencia": "scripts/demo_iceberg_features.py + Bronze→Silver→Gold + 7 tablas Gold",
    },
    {
        "seccion": "4.7",
        "titulo": "Integración batch↔streaming",
        "puntos": 10,
        "estado": "Cumplido",
        "evidencia": "src/streaming/flink_jobs/job_hibrido.py (Lambda explícito) + 3 notebooks Gold",
    },
    {
        "seccion": "4.8",
        "titulo": "Calidad de código (sin hardcodes)",
        "puntos": 5,
        "estado": "Cumplido",
        "evidencia": "iceberg.properties usa ${ENV:VAR}; estructura por capa; convenciones español+snake_case",
    },
    {
        "seccion": "4.9",
        "titulo": "Demo en vivo",
        "puntos": 8,
        "estado": "Listo · depende de la presentación",
        "evidencia": "make all + make cumplimiento-rubrica + make bi-up + this BI dashboard",
    },
    {
        "seccion": "Bonus +2",
        "titulo": "Trino como tercer motor SQL",
        "puntos": 2,
        "estado": "Cumplido",
        "evidencia": "Servicio Trino + make trino-demo + 2 consultas SQL sobre Gold",
    },
    {
        "seccion": "Bonus +1",
        "titulo": "make all end-to-end",
        "puntos": 1,
        "estado": "Cumplido",
        "evidencia": "Makefile target `all`: up + namespaces + batch + legacy + ml/grafo",
    },
    {
        "seccion": "Bonus +1",
        "titulo": "Notebook EDA cruzado sobre 7 tablas Gold",
        "puntos": 1,
        "estado": "Cumplido",
        "evidencia": "notebooks/03_eda_completo.ipynb",
    },
    {
        "seccion": "Bonus +1",
        "titulo": "Spark Structured Streaming",
        "puntos": 1,
        "estado": "Cumplido",
        "evidencia": "src/streaming/structured/siata_a_iceberg_streaming.py — micro-batches 30s a Iceberg",
    },
]


# ----------------------------------------------------------------------
# Persistencia
# ----------------------------------------------------------------------


def escribir_csv(ruta: Path, filas: list[dict]) -> None:
    """Escribe una lista de dicts como CSV con cabecera (UTF-8 con BOM para Excel)."""
    if not filas:
        return
    columnas = list(filas[0].keys())
    with ruta.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columnas, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(filas)
    print(f"  · {ruta.relative_to(RAIZ)}  ({len(filas)} filas)")


def escribir_sqlite(ruta_db: Path, tablas: dict[str, list[dict]]) -> None:
    """Crea (o reemplaza) un SQLite con una tabla por cada conjunto de filas."""
    if ruta_db.exists():
        ruta_db.unlink()
    con = sqlite3.connect(ruta_db)
    try:
        cur = con.cursor()
        for nombre, filas in tablas.items():
            if not filas:
                continue
            columnas = list(filas[0].keys())
            cols_sql = ", ".join(f'"{c}" TEXT' for c in columnas)
            cur.execute(f'CREATE TABLE "{nombre}" ({cols_sql});')
            placeholders = ", ".join(["?"] * len(columnas))
            cur.executemany(
                f'INSERT INTO "{nombre}" VALUES ({placeholders});',
                [tuple(str(f.get(c, "")) for c in columnas) for f in filas],
            )
            print(f"  · SQLite::{nombre}  ({len(filas)} filas)")
        con.commit()
    finally:
        con.close()


def main() -> int:
    DESTINO.mkdir(parents=True, exist_ok=True)
    print(f"Escribiendo metadatos BI en {DESTINO.relative_to(RAIZ)}/")
    print()
    escribir_csv(DESTINO / "hallazgos.csv", HALLAZGOS)
    escribir_csv(DESTINO / "decisiones.csv", DECISIONES)
    escribir_csv(DESTINO / "herramientas.csv", HERRAMIENTAS)
    escribir_csv(DESTINO / "cumplimiento_rubrica.csv", CUMPLIMIENTO_RUBRICA)
    print()
    escribir_sqlite(
        DESTINO / "pulsomed_bi.db",
        {
            "hallazgos": HALLAZGOS,
            "decisiones": DECISIONES,
            "herramientas": HERRAMIENTAS,
            "cumplimiento_rubrica": CUMPLIMIENTO_RUBRICA,
        },
    )
    print()
    print("Listo. Súbelos a Superset con `make bi-init`.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
