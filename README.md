# Pulso Medellín

> Plataforma de datos híbrida (batch + streaming) para la integración de la movilidad urbana del Valle de Aburrá.

**Curso:** ST1630 — Sistemas Intensivos en Datos · Universidad EAFIT
**Equipo:** Jean Carlo Londoño Ocampo · Moisés Vergara Garcés · Alejandro Garcés Ramírez

---

## ¿Qué es esto?

Un sistema que integra seis fuentes de datos públicas de la ciudad (SIATA, Metro de Medellín, EnCicla, SIMM, MEData y GeoMedellín) en una arquitectura **Lakehouse Medallion** (Bronze → Silver → Gold sobre Apache Iceberg + MinIO) combinada con un **camino de streaming** (Kafka + Flink + MongoDB) para responder simultáneamente:

- **Preguntas analíticas** (camino batch): ¿correlación lluvia–afluencia Metro 2018-2024?, ¿corredores de mayor riesgo compuesto?, etc.
- **Preguntas operacionales** (camino streaming): ¿hay alerta PM2.5 en este momento?, ¿hay estaciones EnCicla agotadas cerca de un Metro saturado ahora?, etc.

La pregunta central que articula todo el proyecto:

> ¿Cuándo llueve fuerte en la zona nororiental de Medellín, cuántos minutos de retraso se generan en el Metrocable K y cuántos usuarios migran a EnCicla o Metroplús como alternativa?

Para más detalle del dominio, lee la [propuesta original](./docs/Propuesta_pulsomed_SID.pdf) (cuando la copien aquí).

---

## Quick Start (Sprint 0)

> ⚠️ Antes de seguir, lee `docs/sprints/sprint-0-setup.md` completo. Esto es solo el resumen.

```bash
# 1. Clonar / copiar el proyecto dentro de la carpeta "SISTEMAS INTENSIVOS DE DATOS"
cd "SISTEMAS INTENSIVOS DE DATOS/pulso-medellin"

# 2. Copiar variables de entorno
cp .env.example .env

# 3. Levantar todos los servicios
make up

# 4. Verificar que todo está vivo
make ps
make smoke

# 5. Bajar todo cuando termines
make down
```

Si `make smoke` pasa, el Sprint 0 está terminado y pueden pasar al Sprint 1.

---

## Documentación

| Documento | Para qué sirve |
|-----------|----------------|
| [`docs/00-roadmap.md`](docs/00-roadmap.md) | Plan de los 6 sprints del proyecto. Léelo primero. |
| [`docs/01-arquitectura.md`](docs/01-arquitectura.md) | Diagrama y explicación del stack técnico. |
| [`docs/sprints/sprint-0-setup.md`](docs/sprints/sprint-0-setup.md) | Guía paso a paso del Sprint 0 (este sprint). |
| [`docs/decisiones/`](docs/decisiones/) | ADRs (Architecture Decision Records) — se llenan en sprints futuros. |

---

## Estructura del repositorio

```
pulso-medellin/
├── docker-compose.yml         # Orquestación de TODOS los servicios
├── Makefile                   # Comandos cortos (make up, make smoke, etc.)
├── .env.example               # Variables de entorno (copiar a .env)
├── docs/                      # Toda la documentación
│   ├── 00-roadmap.md          # Plan de sprints
│   ├── 01-arquitectura.md     # Stack y arquitectura
│   ├── sprints/               # Una guía por sprint
│   └── decisiones/            # ADRs (Lambda vs Kappa, Delta vs Iceberg, etc.)
├── src/
│   ├── batch/                 # Pipelines Bronze/Silver/Gold (PySpark)
│   ├── streaming/             # Productores Kafka, jobs Flink, sinks MongoDB
│   └── shared/                # Código compartido (esquemas, utilidades)
├── data/
│   ├── raw/                   # Datasets descargados (gitignored excepto README)
│   ├── samples/               # Muestras pequeñas para test (sí se versionan)
│   └── processed/             # Salidas locales (gitignored)
├── notebooks/                 # EDA y notebooks de análisis exploratorio
├── tests/
│   └── smoke/                 # Pruebas mínimas de que el stack funciona
├── scripts/                   # Scripts auxiliares (init buckets, descargas)
└── docker/                    # Dockerfiles personalizados (Spark con jars, etc.)
```

---

## Convenciones del equipo

- **Idioma del código y comentarios:** español (consistente con el dominio y la propuesta).
- **Idioma de identificadores técnicos** (nombres de tablas Iceberg, tópicos Kafka, colecciones Mongo): **inglés** y `snake_case`. Ej: `bronze.medata_incidentes`, no `bronze.medata_incidentes_de_la_movilidad`.
- **Branching:** `main` siempre estable. Cada sprint en una rama `sprint/N-nombre`. Cada feature en una rama `feat/N-descripcion-corta`.
- **Commits:** convencionales en español. Ej: `feat(bronze): ingesta de incidentes MEData con append por lote`.
- **Docs primero:** ningún módulo se considera terminado sin su `.md` correspondiente que explique qué hace, cómo correrlo, y qué decisión técnica encarna.

---

## Cobertura de los ejes del curso

Cada sprint cubre uno o más ejes temáticos del curso (ver `docs/00-roadmap.md` para el mapeo completo).

| Eje del curso | Sprint donde se cubre |
|---------------|------------------------|
| Hadoop MapReduce (T2) | Sprint 4 — Módulo 01 |
| Formatos de almacenamiento (T3/T4) | Sprint 1 — Módulo 04 (benchmark) |
| Spark MLlib & GraphX (T4) | Sprint 5 — Módulo 06 |
| Lakehouse + Iceberg (T5/T6) | Sprints 1-2 — Módulo 03 |
| Interoperabilidad multi-motor (T6) | Sprint 1 + bonus Trino (Módulo 05) |
| Streaming (Kafka/Flink) | Sprints 2-3 |
| Cloud y gobernanza | Sprint 5 — Módulo 07 |
