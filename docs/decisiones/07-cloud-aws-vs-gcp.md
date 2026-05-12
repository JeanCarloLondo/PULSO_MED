# ADR 07 · Estrategia Cloud: AWS vs GCP + Gobernanza de Datos

**Estado:** Aceptado  
**Módulo del curso:** Módulo 07 — Cloud y Gobernanza  
**Fecha:** 2026-05-12  
**Autores:** Equipo Pulso Medellín (ST1630, EAFIT)

---

## Contexto

El proyecto Pulso Medellín corre actualmente en Docker local (Lakehouse sobre
MinIO + Iceberg REST, Spark, Kafka, MongoDB). Para una puesta en producción real
se necesita:

1. Decidir el proveedor cloud que mejor encaje con la arquitectura Lakehouse ya
   construida.
2. Documentar los controles de acceso por capa Medallion (Bronze/Silver/Gold).
3. Acreditar el cumplimiento de **Ley 1581 de 2012** (protección de datos
   personales) y **Ley 1712 de 2014** (transparencia y acceso a información
   pública), que aplican a las fuentes de datos utilizadas.

---

## Alternativas evaluadas

### AWS

| Servicio local | Equivalente AWS | Notas |
|----------------|-----------------|-------|
| MinIO          | Amazon S3       | Referencia de implementación de Iceberg; protocolo nativo |
| Iceberg REST   | AWS Glue Data Catalog | Soporte nativo Iceberg REST desde Glue 3.0; sin servidor |
| Spark          | Amazon EMR Serverless | Iceberg built-in; pago por uso |
| Kafka          | Amazon MSK      | Managed Kafka; retención configurable |
| MongoDB        | MongoDB Atlas (AWS) | O Amazon DocumentDB compatible |
| MLlib / Spark  | EMR / SageMaker | Pipeline ML integrado |
| Red privada    | VPC + IAM       | Políticas por rol y por recurso |

**Fortalezas:**
- S3 es la implementación de referencia de Apache Iceberg (todas las pruebas de
  rendimiento del proyecto usan semántica S3).
- AWS Glue Catalog implementa la especificación REST Catalog de Iceberg sin
  código adicional — migración directa desde `docker-compose.yml`.
- IAM + Lake Formation permite control de acceso a nivel de columna y fila, lo
  que cubre los requisitos de Ley 1581 para `id_usuario` EnCicla.
- Mayor cuota de mercado en Colombia: ~47% (Gartner, 2024); más proveedores
  locales de soporte.

**Debilidades:**
- Costos de egress de datos (salida de S3) acumulan en proyectos con muchas
  lecturas externas.
- SageMaker añade complejidad respecto a EMR puro para pipelines MLlib ya
  implementados en PySpark.

### GCP

| Servicio local | Equivalente GCP | Notas |
|----------------|-----------------|-------|
| MinIO          | Google Cloud Storage (GCS) | Compatible con Iceberg vía `gcs://` |
| Iceberg REST   | BigLake Metastore | Soporte REST Catalog (GA desde 2024) |
| Spark          | Dataproc Serverless | PySpark nativo; integración Iceberg |
| Kafka          | Pub/Sub + Dataflow | Diferente paradigma; mayor refactor |
| MongoDB        | MongoDB Atlas (GCP) | O Firestore (cambio de paradigma) |
| MLlib / Spark  | Dataproc / Vertex AI | Vertex AI para modelos en producción |
| Red privada    | VPC + IAM + VPC-SC | IAM por recurso; VPC Service Controls |

**Fortalezas:**
- Vertex AI tiene pipeline de ML más maduro para despliegue de modelos PySpark.
- BigQuery como motor analítico adicional (complementario a Trino sobre Iceberg).
- Cuotas de free-tier más generosas para proyectos académicos.

**Debilidades:**
- Pub/Sub tiene semántica diferente a Kafka; migrar los 4 topics requeriría
  refactorizar todos los productores y jobs de Sprint 2/3.
- BigLake Metastore con REST Catalog es más reciente y menos documentado que
  AWS Glue; riesgo de incompatibilidades con la versión de Iceberg usada.
- Menor presencia de integradores Dataproc/Iceberg en el ecosistema colombiano.

---

## Decisión

**Se adopta AWS** como proveedor cloud objetivo.

### Justificación

1. **Compatibilidad directa con la arquitectura existente:** la pila local
   (MinIO → S3, Iceberg REST → Glue, Spark → EMR) migra con cambios mínimos
   de configuración, únicamente modificando URIs y credenciales en
   `src/shared/config.py`.

2. **Kafka → MSK sin cambios de código:** los productores y jobs del Sprint 2/3
   solo cambian `KAFKA_BOOTSTRAP_SERVERS`; la API es idéntica.

3. **Iceberg nativo en S3:** evita el overhead de traducción GCS↔S3 que
   introduce latencia y costos adicionales en lecturas de Iceberg.

4. **IAM + Lake Formation:** permite implementar exactamente los controles
   de acceso por capa documentados en la sección siguiente, a nivel de columna,
   lo que es el mecanismo adecuado para cumplir Ley 1581.

### Mapa de migración

```
Componente local          → Servicio AWS
──────────────────────────────────────────────────────
MinIO (S3 local)          → Amazon S3  (bucket pulsomed-warehouse)
Iceberg REST Catalog      → AWS Glue Data Catalog (modo REST)
Spark (docker)            → EMR Serverless 6.15+  (Iceberg built-in)
Zookeeper + Kafka         → Amazon MSK  (Apache Kafka managed)
MongoDB                   → MongoDB Atlas on AWS (us-east-1)
stream-runner (Python)    → AWS Lambda o ECS Fargate
Streamlit (dashboard)     → AWS App Runner o Amplify
```

---

## Controles de acceso por capa (Módulo 07)

La política de acceso sigue el principio de mínimo privilegio por rol:

| Capa     | Rol lector                  | Rol escritor           | Restricciones de columna |
|----------|-----------------------------|------------------------|--------------------------|
| Bronze   | Ingenieros de datos (batch) | Pipelines ETL (batch)  | `id_usuario` EnCicla: solo HMAC hash visible |
| Silver   | Ingenieros de datos         | Pipelines ETL          | Ninguna adicional (PII ya pseudonimizado) |
| Gold     | Analistas, data scientists  | Pipelines Gold         | Sin restricciones (sin PII) |
| Streaming (Kafka) | Jobs streaming    | Productores            | ACLs por topic; consumidores solo al topic propio |
| MongoDB  | Dashboard, CLI consulta     | Jobs streaming         | Autenticación por colección |

En AWS, esto se implementa con:
- **S3 Bucket Policies** + **IAM Roles** por servicio (una rol por ETL, una por
  analítica).
- **Glue Data Catalog Resource Policies** para controlar qué roles pueden hacer
  `GetTable` en Bronze vs Gold.
- **AWS Lake Formation** para restricciones a nivel de columna en `id_usuario`
  (aunque ya viene pseudonimizado desde el ingest, como capa adicional).

---

## Cumplimiento Ley 1581 de 2012 (Protección de datos personales)

La Ley 1581 aplica porque EnCicla procesa datos de usuarios identificables
(`id_usuario` de préstamos de bicicleta pública).

| Requisito Ley 1581                | Implementación en Pulso Medellín |
|-----------------------------------|----------------------------------|
| Principio de finalidad            | Los datos se usan exclusivamente para análisis de movilidad urbana (propósito declarado en propuesta ST1630). |
| Principio de libertad             | Solo se procesan datos de fuentes públicas (AMVA/EnCicla) publicados bajo Ley 1712. |
| Principio de seguridad            | `id_usuario` se pseudonimiza con HMAC-SHA256 **antes de llegar a Bronze** (ver `src/batch/bronze/ingest_encicla.py`). La clave `HMAC_USER_PSEUDO_SECRET` vive en `.env` y nunca en código ni commits. |
| Principio de acceso y corrección  | La pseudonimización es unidireccional: el sistema no mantiene la tabla de correspondencia `id_real ↔ id_hash`, por lo que no puede re-identificar usuarios. |
| Transferencia internacional       | En migración a AWS, los datos residen en `us-east-1`. Para cumplimiento estricto de Ley 1581, se recomienda usar la región `sa-east-1` (São Paulo) o esperar a que AWS habilite una región colombiana. |

### Datos que NO son personales (fuera de alcance Ley 1581)

- MEData incidentes viales: coordenadas y características del accidente, sin
  identificación de personas involucradas.
- SIATA lecturas de calidad del aire: datos ambientales por estación, sin PII.
- SIMM aforos vehiculares: conteos agregados por corredor, sin PII.
- Metro afluencia: conteos por línea y hora, sin PII.
- GeoMedellín geometrías de comunas: datos geográficos públicos.

---

## Cumplimiento Ley 1712 de 2014 (Transparencia y acceso a información pública)

La Ley 1712 aplica porque las fuentes de datos son de entidades públicas
(MEData, SIATA, Metro de Medellín, AMVA, Alcaldía de Medellín) y el proyecto
las redistribuye en un formato derivado.

| Requisito Ley 1712                | Implementación |
|-----------------------------------|----------------|
| Información mínima publicada      | Todas las fuentes tienen URL pública y licencia abierta documentada en `docs/01-arquitectura.md`. |
| Acceso a datos primarios          | Los CSV/XLSX crudos se archivan en `data/raw/` (gitignored por tamaño, pero reproducibles vía `make download-data`). |
| Metadatos de calidad              | Cada tabla Bronze incluye columnas de auditoría: `timestamp_ingesta`, `nombre_archivo`, `fuente_id` (ver `src/shared/bronze_utils.py`). |
| No restricción de formatos        | Las tablas Gold son consultables desde Spark, Jupyter, Trino (bonus) y el dashboard Streamlit — múltiples interfaces. |
| Gratuidad                         | El proyecto es académico y sin costo de acceso. En producción, los controles de acceso por rol no cobran a lectores autorizados. |

---

## Consecuencias

- **Positivas:** migración directa con mínimos cambios de configuración; IAM +
  Lake Formation cubre los requisitos de Ley 1581 sin código adicional.
- **Negativas:** egress de S3 puede acumular costo si el dashboard hace lecturas
  frecuentes; se mitiga con caching en MongoDB (ya existente en el diseño).
- **Neutras:** la decisión no afecta el código actual — toda la configuración
  cloud vive en variables de entorno que ya están abstraídas en `shared/config.py`.

---

## Referencias

- Apache Iceberg REST Catalog Spec: https://iceberg.apache.org/rest-catalog
- AWS Glue Catalog REST support: https://docs.aws.amazon.com/glue/
- AWS Lake Formation column-level security: https://docs.aws.amazon.com/lake-formation/
- Ley 1581 de 2012 (Colombia): https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=49981
- Ley 1712 de 2014 (Colombia): https://www.secretariatransparencia.gov.co/ley-1712
