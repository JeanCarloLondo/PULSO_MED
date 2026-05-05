# Arquitectura del Sistema · Pulso Medellín

> Para el detalle de **por qué** elegimos cada tecnología, ver la sección 5 de la propuesta original. Este documento es la versión operacional: **qué corre dónde, cómo se conecta, y cómo razonamos sobre ello**.

---

## Vista general (camino batch + streaming)

```mermaid
flowchart LR
  subgraph Fuentes
    A1[MEData<br/>incidentes viales<br/>CSV histórico]
    A2[Metro<br/>afluencia + GTFS<br/>CSV/GeoJSON]
    A3[EnCicla<br/>préstamos + disponibilidad<br/>CSV + API]
    A4[SIATA<br/>meteorología<br/>JSON + Kaggle]
    A5[GeoMedellín<br/>comunas/barrios<br/>GeoJSON/Shapefile]
    A6[SIMM<br/>aforos vehiculares<br/>CSV + simulado]
  end

  subgraph Batch [Camino Batch · Lakehouse]
    B1[Spark<br/>PySpark]
    B2[(MinIO<br/>S3 API)]
    B3[Iceberg<br/>REST Catalog]
    B4[Bronze<br/>Iceberg]
    B5[Silver<br/>Iceberg]
    B6[Gold<br/>Iceberg]
  end

  subgraph Streaming [Camino Streaming · RT]
    C1[Kafka<br/>+ Zookeeper]
    C2[Flink<br/>Job/Task Mgr]
    C3[(MongoDB)]
  end

  subgraph Consumo
    D1[Notebook<br/>EDA Gold]
    D2[Trino<br/>3er motor SQL]
    D3[Dashboard<br/>Streamlit]
    D4[Job híbrido<br/>Gold→RT]
  end

  A1 --> B1
  A2 --> B1
  A3 --> B1
  A4 --> B1
  A5 --> B1
  A6 --> B1

  A2 --> C1
  A3 --> C1
  A4 --> C1
  A6 --> C1

  B1 -- writes --> B4 -- transforms --> B5 -- aggregates --> B6
  B4 -.metadata.- B3
  B5 -.metadata.- B3
  B6 -.metadata.- B3
  B3 -.warehouse.- B2

  C1 --> C2 --> C3

  B6 --> D1
  B6 --> D2
  C3 --> D3
  B6 --> D4
  C2 --> D4
  D4 --> C3
```

---

## Servicios desplegados (Docker Compose)

Todos los servicios corren en una sola red Docker. La red se llama `pulsomed-net`. Los nombres de host coinciden con los nombres de servicio de Compose, así que dentro de la red `minio` es un hostname válido, igual que `kafka`, `iceberg-rest`, etc.

| Servicio | Imagen | Puerto host | Para qué |
|----------|--------|-------------|----------|
| `minio` | `minio/minio:latest` | 9000 (API), 9001 (Console) | Almacenamiento de objetos S3-compatible. Es el "warehouse" físico del Lakehouse. |
| `mc` | `minio/mc:latest` | — | Job de inicialización: crea el bucket `warehouse` en MinIO al arranque. Termina apenas lo crea. |
| `iceberg-rest` | `tabulario/iceberg-rest:1.5.0` | 8181 | Catálogo Iceberg expuesto como REST. Spark, Trino, y notebooks lo consultan para descubrir tablas. |
| `spark-iceberg` | `tabulario/spark-iceberg:latest` | 8888 (Jupyter), 8080 (Spark UI), 10000 (Thrift) | Spark con jars de Iceberg pre-instalados. Sirve también un Jupyter para EDA. |
| `mongodb` | `mongo:7` | 27017 | Sink del streaming. NoSQL documental para alertas y agregados RT. |
| `mongo-express` *(opcional)* | `mongo-express:latest` | 8082 | UI web para inspeccionar MongoDB. Solo en dev. |
| `zookeeper` *(Sprint 2+)* | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Coordinación de Kafka. |
| `kafka` *(Sprint 2+)* | `confluentinc/cp-kafka:7.5.0` | 9092, 29092 | Broker. |
| `kafka-ui` *(Sprint 2+, opcional)* | `provectuslabs/kafka-ui:latest` | 8083 | UI web para inspeccionar tópicos y consumir mensajes. |
| `flink-jobmanager` *(Sprint 2+)* | `flink:1.18` | 8081 | UI y coordinación de jobs. |
| `flink-taskmanager` *(Sprint 2+)* | `flink:1.18` | — | Workers de procesamiento. |
| `trino` *(Sprint 5, bonus)* | `trinodb/trino:latest` | 8084 | Tercer motor SQL sobre Gold. Demuestra interoperabilidad de Iceberg. |

> **Nota sobre puertos:** Trino y Spark UI ambos quieren `:8080` por defecto. Los remapeamos: Spark UI a `:8080` (host), Trino a `:8084` (host). Flink también pelea por `:8080` a veces; lo dejamos en `:8081`.

### ¿Por qué `tabulario/spark-iceberg`?

Es la imagen mantenida por Tabular (los creadores del REST Catalog) que ya trae:
- Spark 3.5 con configuración Iceberg.
- Jars de `iceberg-spark-runtime`, `aws-bundle`, `bundle` AWS S3 SDK ya instalados.
- Jupyter Lab con kernel PySpark listo.
- Variables de entorno preconfiguradas para apuntar al REST Catalog y a un endpoint S3 personalizado.

Esto nos ahorra **horas** de pelear con classpath de Spark + jars compatibles. Si después del Sprint 1 necesitamos personalizar (añadir jars de Kafka connect, MongoDB Spark connector, etc.), creamos un Dockerfile en `docker/spark/` que extiende esta imagen.

---

## Configuración Iceberg (clave)

El namespace que usaremos en Iceberg es `pulsomed`. Las tablas se nombran:

```
pulsomed.bronze.<fuente>      -- ej: pulsomed.bronze.medata_incidentes
pulsomed.silver.<entidad>     -- ej: pulsomed.silver.incidentes_geocodificados
pulsomed.gold.<metrica>       -- ej: pulsomed.gold.afluencia_vs_pm25
```

El warehouse físico vive en `s3://warehouse/pulsomed/` (MinIO). El REST Catalog mapea cada tabla a su ubicación dentro de ese bucket.

### Acceso desde Spark

```python
spark = (
    SparkSession.builder
    .appName("PulsoMedellin")
    .config("spark.sql.catalog.pulsomed", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.pulsomed.type", "rest")
    .config("spark.sql.catalog.pulsomed.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.pulsomed.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.pulsomed.s3.endpoint", "http://minio:9000")
    .config("spark.sql.defaultCatalog", "pulsomed")
    .getOrCreate()
)
```

> En la imagen `tabulario/spark-iceberg`, esto ya viene configurado vía variables de entorno; no hay que pasarlo en cada `SparkSession.builder`. Pero documentar es importante para cuando migremos a la nube.

---

## Decisión: ¿cuándo MongoDB y cuándo Gold?

Esta tabla resume la sección 4.4 de la propuesta y guía toda decisión de diseño:

| Criterio | MongoDB (operacional) | Gold Iceberg (analítica) |
|----------|----------------------|--------------------------|
| **Latencia** | < 10 ms (lectura por clave primaria) | Segundos a minutos (scan + agregación) |
| **Cardinalidad típica** | Baja: últimas N alertas, una estación | Alta: millones de filas históricas |
| **Patrón de escritura** | Continua, sin bloqueo de esquema | Micro-batch / batch nocturno, ACID |
| **Pregunta ejemplo** | "¿cuántas bicis hay en El Poblado AHORA?" | "¿correlación PM2.5–afluencia 2018-2024?" |

**Regla de oro:** si la respuesta requiere mirar más de 10 minutos de historia o cruzar más de una fuente, va a Gold. Si la respuesta es "estado actual de X" o "alertas de los últimos N minutos", va a MongoDB.

---

## Diagrama de Bronze → Silver → Gold

```mermaid
flowchart TD
  subgraph Bronze [Bronze · sin transformar]
    B1[medata_incidentes<br/>partición: fecha_ingesta]
    B2[metro_afluencia]
    B3[encicla_prestamos]
    B4[siata_lecturas]
    B5[siata_realtime_raw]
    B6[geomedellin_comunas]
    B7[simm_aforos]
  end

  subgraph Silver [Silver · limpio + enriquecido]
    S1[incidentes_geocodificados<br/>+ comuna + barrio]
    S2[afluencia_horaria<br/>+ pm25 + precip]
    S3[viajes_encicla_anonimizados]
    S4[lecturas_aire_validas]
    S5[aforos_corredor_geo]
  end

  subgraph Gold [Gold · métricas de negocio]
    G1[afluencia_vs_pm25 → B-1]
    G2[accidentalidad_por_comuna → B-2]
    G3[demanda_encicla_vs_clima → B-3]
    G4[corredores_riesgo_compuesto → B-4]
  end

  B1 --> S1
  B6 --> S1
  B2 --> S2
  B4 --> S2
  B3 --> S3
  B6 --> S3
  B4 --> S4
  B5 --> S4
  B7 --> S5
  B6 --> S5

  S2 --> G1
  S1 --> G2
  S3 --> G3
  S4 --> G3
  S5 --> G4
  S1 --> G4
```

---

## Variables de entorno

Todas las contraseñas y endpoints viven en un único archivo `.env` en la raíz del repo. **Este archivo está en `.gitignore`**. Lo que se versiona es `.env.example` con valores de placeholder.

Ver `.env.example` para la lista completa.

---

## ¿Qué falta documentar?

Estos diagramas se completan en sprints posteriores:

- [ ] Diagrama de tópicos Kafka y consumidores (Sprint 2).
- [ ] Diagrama de jobs Flink con sus ventanas y salidas (Sprint 2-3).
- [ ] Diagrama del job híbrido Gold↔Streaming (Sprint 3).
- [ ] Diagrama de despliegue cloud (Sprint 5).
