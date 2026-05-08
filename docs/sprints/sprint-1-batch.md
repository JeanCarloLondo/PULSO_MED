# Sprint 1 · Camino Batch (Bronze → Silver → Gold)

> Estado: cerrado. MVP `make pipeline-batch` corre los 6 ingest, las 5 transformaciones Silver y los 4 builds Gold de un tirón. Las 4 tablas Gold responden las preguntas analíticas B-1..B-4.

## Lo que hay que correr (en una máquina nueva)

```bash
# 1. Stack arriba (heredado de Sprint 0)
make up
make smoke                # opcional: valida MinIO+Iceberg+Mongo

# 2. Datos crudos (~1 GB en disco; descarga ~5-10 min)
make download-data        # MEData, SIMM (reales) + warnings para los demás

# 3. Sustituir las fuentes que el portal NO entrega vía descarga directa
PYTHONIOENCODING=utf-8 python scripts/generar_muestras_sinteticas.py
# (o:  make generate-samples)

# 4. Pipeline completo
make init-namespaces      # crea pulsomed.{bronze,silver,gold} en Iceberg
make pipeline-batch       # Bronze → Silver → Gold
```

Al final, las cuatro tablas Gold quedan en `s3://warehouse/pulsomed/gold/`.

## Origen de cada fuente

| Fuente | Estado descarga real | Notas |
|--------|----------------------|-------|
| MEData incidentes | ✅ CSV oficial (60 MB, 270 k filas) | medata.gov.co directo |
| SIMM aforos + cámaras | ✅ CSV oficial (~835 MB; cámaras se carga limitado a 300 k filas con `SIMM_LIMIT_FILAS`) | medata.gov.co |
| GeoMedellín comunas | ✅ Generado vía `scripts/overpass_a_geojson.py` (OSM Overpass) | 21 polígonos: 16 comunas + 5 corregimientos |
| Metro afluencia | 🟡 Sintético (ArcGIS Hub bloquea curl con 403) | Esquema canónico; `data/raw/metro_afluencia/afluencia_metro_{2022,2023,2024}.csv` |
| EnCicla préstamos | 🟡 Sintético (préstamos históricos requieren PQRS; API CKAN cambió URLs) | 13 776 préstamos, 6 meses, ya pseudonimizados a Bronze |
| SIATA PM2.5 | 🟡 Sintético (Dataverse requiere `jq` para descarga bulk) | 1 año horario; estructura idéntica a la del stream Sprint 2 |

> Reemplazar por datos reales: dejar los archivos en `data/raw/<fuente>/` con los nombres documentados y volver a correr `make pipeline-batch`. Los scripts Bronze son agnósticos al origen.

## Qué hay en cada capa

### Bronze (`pulsomed.bronze.*`)

Append puro con 4 columnas de auditoría (`timestamp_ingesta`, `nombre_archivo`, `fuente_id`, `fecha_ingesta`) y particionado por `fecha_ingesta`. Sin transformaciones.

| Tabla | Filas tras Sprint 1 |
|-------|---------------------|
| `geomedellin_comunas` | 21 |
| `metro_afluencia` | 29 592 |
| `siata_lecturas` | 87 600 |
| `encicla_prestamos` | 13 776 (con `usuario_pseudo` HMAC-SHA256, sin `id_usuario`) |
| `medata_incidentes` | 270 765 |
| `simm_aforos` | 374 900 (manual + cámaras) |

EnCicla: ya en Bronze los IDs de usuario se reemplazan por HMAC-SHA256 con la clave de `.env`. **No se ingresa nunca un id en claro.**

### Silver (`pulsomed.silver.*`)

Una transformación por entidad. Replace puro (`createOrReplace`) — Silver es derivable, no acumulable.

| Tabla | Lógica |
|-------|--------|
| `lecturas_aire_validas` | SIATA con `-999 → NULL`, filtrado de PM2.5 nulo, casteo timestamps |
| `afluencia_horaria` | Metro diario × promedio diario de PM2.5/lluvia/temperatura de SIATA |
| `incidentes_geocodificados` | MEData con: corrección de coordenadas invertidas (heurística pre-2017), bbox del Valle, dedup por `nro_radicado`, comuna por point-in-polygon (UDF Python contra GeoMedellín) |
| `viajes_encicla_anonimizados` | Préstamos limpios + categoría duración + clima del día |
| `aforos_corredor_geo` | Unión SIMM manual+cámaras con coords y comuna asignada por point-in-polygon |

El join espacial se hace con un UDF Python clásico (ray-casting). No usamos Sedona — no hace falta para 21 polígonos y ~600 k puntos.

### Gold (`pulsomed.gold.*`)

| Tabla | Pregunta | Granularidad |
|-------|----------|--------------|
| `afluencia_vs_pm25` | B-1: correlación PM2.5/lluvia ↔ afluencia | estación × mes (con `corr_pm25_validaciones`, `corr_precip_validaciones`) |
| `accidentalidad_por_comuna` | B-2: severidad por comuna | comuna × año, pivot por gravedad, `indice_severidad` ponderado |
| `demanda_encicla_vs_clima` | B-3: elasticidad de demanda | día × bin temperatura × bin PM2.5 × indicador lluvia, `viajes_relativos_pct` vs media |
| `corredores_riesgo_compuesto` | B-4: corredores más peligrosos | comuna, ranking por volumen + severidad |

## Decisiones técnicas

1. **Catálogo Iceberg `demo`, no `pulsomed`.** La imagen `tabulario/spark-iceberg` ya viene cableada al catálogo `demo`. `pulsomed` es un namespace bajo ese catálogo. Cambiar el catálogo requeriría tunear variables de entorno de la imagen.

2. **Particionado: `fecha_ingesta` en Bronze, `anio_*` en Silver/Gold.** Bronze prioriza la idempotencia del append; Silver/Gold prioriza el partition pruning para queries por año.

3. **Ray-casting Python en vez de Apache Sedona.** Sedona requiere otro classpath y un Dockerfile custom. Para Sprint 1 (21 polígonos) el Python UDF es suficiente. Si en Sprint 5 necesitamos joins espaciales más sofisticados, migramos.

4. **`SIMM_LIMIT_FILAS` por defecto a 300 k.** El CSV de cámaras es 816 MB y tiene ~3M filas. Para iterar rápido en Sprint 1 limitamos. Para cargar completo: `SIMM_LIMIT_FILAS=99999999 make ingest-bronze-simm`.

5. **EnCicla pseudonimizado en Bronze, no en Silver.** Quien tiene acceso al data lake nunca debe ver `id_usuario` real, ni siquiera en una capa intermedia. La clave HMAC vive solo en `.env`.

6. **`createOrReplace` en Silver/Gold.** Idempotente y simple. Si en Sprint 4 necesitamos historial de cambios, agregamos snapshot retention en Iceberg.

## Verificación rápida

```bash
docker compose exec spark-iceberg python -c "
import sys; sys.path.insert(0,'/workspace/src')
from shared.config import *
from shared.config import crear_spark_session
spark = crear_spark_session('check'); spark.sparkContext.setLogLevel('ERROR')
for t in (TBL_GOLD_AFLUENCIA_PM25, TBL_GOLD_ACCIDENTALIDAD,
          TBL_GOLD_ENCICLA_CLIMA, TBL_GOLD_CORREDORES_RIESGO):
    print(f'{t}: {spark.table(t).count():,} filas')
"
```

Salida esperada:

```
demo.pulsomed.gold.afluencia_vs_pm25:           972 filas
demo.pulsomed.gold.accidentalidad_por_comuna:   220 filas
demo.pulsomed.gold.demanda_encicla_vs_clima:    180 filas
demo.pulsomed.gold.corredores_riesgo_compuesto:  16 filas
```

## EDA

`notebooks/01_eda_gold.ipynb` muestra las 4 respuestas con gráficas. Abrir en `http://localhost:8888`.

## Pendiente para Sprint 1.5 / Sprint 4

- [ ] Cuando lleguen los CSV reales de EnCicla (vía PQRS), reemplazar el sintético y re-correr Silver+Gold.
- [ ] Cargar la totalidad de `simm_traffic_data.csv` (3M filas) — requiere subir el heap de Spark.
- [ ] Reemplazar el ray-casting Python por broadcast join con Sedona si Sprint 5 necesita más precisión geo.
- [ ] Documentar el benchmark de formatos en `docs/decisiones/04-benchmark-formatos.md` (Módulo 04).
