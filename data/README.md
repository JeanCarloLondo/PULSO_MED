# `data/` — Datasets

## Estructura

| Carpeta | Versionado | Contenido |
|---------|-----------|-----------|
| `raw/` | ❌ NO (gitignored) | Datasets crudos descargados de las fuentes (MEData, SIATA, etc). Pueden pesar GBs. |
| `samples/` | ✅ SÍ | Muestras pequeñas (< 1 MB cada una) para tests unitarios y CI. |
| `processed/` | ❌ NO | Salidas locales de pruebas que no son la verdad oficial (la verdad vive en MinIO). |

## Origen y descarga (Sprint 1)

En el Sprint 1 crearemos `scripts/download_datasets.sh` (o similar en Python) que descarga las fuentes públicas a `data/raw/`. **Hasta entonces**, esta carpeta queda vacía.

| Fuente | Origen oficial | Tamaño aprox | Ley aplicable |
|--------|----------------|--------------|----------------|
| MEData incidentes viales | https://medata.gov.co (Socrata API) | ~150 MB CSV | Ley 1712 de 2014 |
| Metro afluencia + GTFS | datosabiertos-metrodemedellin.opendata.arcgis.com | ~500 MB | Ley 1712 de 2014 |
| EnCicla préstamos + API | datosabiertos.metropol.gov.co | ~200 MB CSV + API | Ley 1712 + Ley 1581 (datos personales) |
| SIATA histórico | Kaggle (años 2017-2023) | ~1-2 GB | Ley 1712 |
| SIATA tiempo real | siata.gov.co (JSON) | streaming | Ley 1712 |
| GeoMedellín | geomedellin.gov.co | ~50 MB GeoJSON | Ley 1712 |
| SIMM aforos | medata.gov.co (tableros SIMM) | ~100 MB CSV | Ley 1712 |

## Privacidad y EnCicla

Antes de cualquier ingesta a Bronze, el campo `id_usuario` de EnCicla se
**pseudonimiza con HMAC-SHA256** (clave en `.env`, ver `HMAC_USER_PSEUDO_SECRET`).
La clave de reversión vive fuera del Lakehouse y no es accesible desde notebooks.
Ver sección 7 de la propuesta y el ADR 07 (Sprint 5).
